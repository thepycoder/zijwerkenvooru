use reqwest::{header, Client, Error, Response};

pub struct ScrapingClient {
    client: Client
}

impl ScrapingClient {
    pub fn new() -> Self {
        let client = Client::builder()
            .build().unwrap();

        ScrapingClient {
            client
        }
    }

    pub async fn get(&self, url: &str) -> Result<Response, Error> {
        let response = self
            .client
            .get(url)
            .headers(self.headers())
            .send()
            .await;

        response
    }

    fn headers(&self) -> header::HeaderMap {
        let mut headers = header::HeaderMap::new();
        headers.insert(header::USER_AGENT, header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"));
        headers.insert(header::ACCEPT, header::HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"));
        headers.insert(header::ACCEPT_LANGUAGE, header::HeaderValue::from_static("en-US,en;q=0.9,nl;q=0.8"));
        headers.insert(header::CONNECTION, header::HeaderValue::from_static("keep-alive"));
        headers.insert(header::UPGRADE_INSECURE_REQUESTS, header::HeaderValue::from_static("1"));
        headers.insert(header::REFERER, header::HeaderValue::from_static("https://www.dekamer.be"));
        // headers.insert(header::FROM, header::HeaderValue::from_static("alexanderameye@gmail.com"));

        headers
    }
}