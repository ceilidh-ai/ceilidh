//! The repository picker's data: every repository the operator can reach,
//! grouped by owner and led by whatever was pushed to most recently.
//!
//! GitHub is asked once and the answer is cached, because the list changes on
//! the timescale of a working day and the new-session form is opened often.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ceilidh_protocol::{RepoChoice, RepoList, RepoOwner};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::sync::Mutex;

const CACHE_TTL: Duration = Duration::from_secs(300);
const PER_PAGE: usize = 100;
const MAX_PAGES: usize = 5;
const USER_AGENT: &str = "ceilidh";

#[derive(Debug, Clone)]
pub struct RepoCatalog {
    token: Option<Arc<str>>,
    client: reqwest::Client,
    cache: Arc<Mutex<Option<(Instant, RepoList)>>>,
}

#[derive(Debug, Deserialize)]
struct GhRepo {
    full_name: String,
    name: String,
    owner: GhOwner,
    #[serde(default)]
    private: bool,
    clone_url: String,
    #[serde(default)]
    pushed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    archived: bool,
}

#[derive(Debug, Deserialize)]
struct GhOwner {
    login: String,
}

impl RepoCatalog {
    pub fn new(token: Option<String>) -> Self {
        Self {
            token: token.filter(|t| !t.trim().is_empty()).map(Arc::from),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap_or_default(),
            cache: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn list(&self) -> RepoList {
        let Some(token) = self.token.clone() else {
            return RepoList {
                owners: Vec::new(),
                available: false,
                error: None,
            };
        };

        {
            let cache = self.cache.lock().await;
            if let Some((fetched, list)) = cache.as_ref() {
                if fetched.elapsed() < CACHE_TTL {
                    return list.clone();
                }
            }
        }

        let list = match self.fetch(&token).await {
            Ok(owners) => RepoList {
                owners,
                available: true,
                error: None,
            },
            Err(err) => RepoList {
                owners: Vec::new(),
                available: false,
                error: Some(err.to_string()),
            },
        };

        if list.available {
            *self.cache.lock().await = Some((Instant::now(), list.clone()));
        }
        list
    }

    async fn fetch(&self, token: &str) -> anyhow::Result<Vec<RepoOwner>> {
        let mut repos: Vec<GhRepo> = Vec::new();
        for page in 1..=MAX_PAGES {
            let url = format!(
                "https://api.github.com/user/repos?affiliation=owner,organization_member\
                 &sort=pushed&direction=desc&per_page={PER_PAGE}&page={page}"
            );
            let response = self
                .client
                .get(url)
                .bearer_auth(token)
                .header("User-Agent", USER_AGENT)
                .header("Accept", "application/vnd.github+json")
                .send()
                .await?;
            if !response.status().is_success() {
                anyhow::bail!("github returned {}", response.status());
            }
            let page_repos: Vec<GhRepo> = response.json().await?;
            let count = page_repos.len();
            repos.extend(page_repos);
            if count < PER_PAGE {
                break;
            }
        }

        Ok(group_by_owner(repos))
    }
}

fn group_by_owner(repos: Vec<GhRepo>) -> Vec<RepoOwner> {
    let mut owners: Vec<RepoOwner> = Vec::new();
    for repo in repos.into_iter().filter(|r| !r.archived) {
        let choice = RepoChoice {
            full_name: repo.full_name,
            owner: repo.owner.login.clone(),
            name: repo.name,
            private: repo.private,
            url: repo.clone_url,
            pushed_at: repo.pushed_at,
        };
        match owners.iter_mut().find(|o| o.login == repo.owner.login) {
            Some(owner) => owner.repos.push(choice),
            None => owners.push(RepoOwner {
                login: repo.owner.login,
                repos: vec![choice],
            }),
        }
    }

    // Owners lead with whoever was pushed to most recently, and so do their
    // repositories: the thing you touched last is the thing you want next.
    for owner in &mut owners {
        owner.repos.sort_by(|a, b| b.pushed_at.cmp(&a.pushed_at));
    }
    owners.sort_by(|a, b| {
        let latest = |o: &RepoOwner| o.repos.first().and_then(|r| r.pushed_at);
        latest(b).cmp(&latest(a))
    });
    owners
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn repo(owner: &str, name: &str, pushed: i64, archived: bool) -> GhRepo {
        GhRepo {
            full_name: format!("{owner}/{name}"),
            name: name.to_string(),
            owner: GhOwner {
                login: owner.to_string(),
            },
            private: true,
            clone_url: format!("https://github.com/{owner}/{name}.git"),
            pushed_at: Some(Utc.timestamp_opt(pushed, 0).unwrap()),
            archived,
        }
    }

    #[test]
    fn owners_and_repos_lead_with_the_most_recent_push() {
        let owners = group_by_owner(vec![
            repo("someone", "old-thing", 100, false),
            repo("org", "fresh", 900, false),
            repo("someone", "newer", 500, false),
            repo("org", "stale", 200, false),
        ]);
        assert_eq!(
            owners.iter().map(|o| o.login.as_str()).collect::<Vec<_>>(),
            vec!["org", "someone"]
        );
        assert_eq!(
            owners[0]
                .repos
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>(),
            vec!["fresh", "stale"]
        );
        assert_eq!(owners[1].repos[0].name, "newer");
    }

    #[test]
    fn archived_repositories_are_left_out() {
        let owners = group_by_owner(vec![
            repo("someone", "live", 100, false),
            repo("someone", "archived", 900, true),
        ]);
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].repos.len(), 1);
        assert_eq!(owners[0].repos[0].name, "live");
    }

    #[tokio::test]
    async fn no_token_means_the_picker_is_simply_unavailable() {
        let list = RepoCatalog::new(None).list().await;
        assert!(!list.available);
        assert!(list.owners.is_empty());
        assert!(list.error.is_none());
    }
}
