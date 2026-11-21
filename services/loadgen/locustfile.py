from locust import HttpUser, between, task


class Visitor(HttpUser):
    wait_time = between(0.5, 2)

    @task
    def index(self) -> None:
        self.client.get("/")
