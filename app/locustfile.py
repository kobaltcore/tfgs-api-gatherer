import time
import random
from locust import HttpUser, task, between


class QuickstartUser(HttpUser):
    wait_time = between(1, 2.5)

    @task
    def view_game(self):
        game_id = random.randint(1, 1750)
        self.client.get(f"/games/{game_id}")
        self.client.get(f"/reviews/list/{game_id}")

    @task
    def search(self):
        query = "likes:>100"
        limit = 10
        for i in range(5):
            self.client.get(f"/search?query={query}&offset={i * limit}&limit={limit}", name="/item")
            time.sleep(1)

    def on_start(self):
        self.client.get("/games/list/trending")
        self.client.get("/games/list/updated")
        self.client.get("/games/list/new")
