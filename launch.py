import random
from time import sleep
import pprint

import requests

from construct import ApiConnector, get_json
from async_framework import Framework
from Queue import Queue


pretty = pprint.PrettyPrinter(indent=2)

API_V1 = '/api/v1/scheduler'
DOCKER_JSON = "./resources/container.json"
LAUNCH_JSON = "./resources/launch.json"
TASK_RESOURCES_JSON = "./resources/task_resources.json"

class Launcher:
    def __init__(self, master_url):
        self.conn = None
        self.background_thread = None
        self.master_url = master_url
        self.api_url = '{}/{}'.format(master_url, API_V1)

    def connect(self):
        r = requests.get("{}/state.json".format(self.master_url))
        self.conn = ApiConnector()
        #self.background_thread = self.conn.register_framework()
        self.framework = Framework(self.conn, self.api_url)
        self.framework.start()

    def wait_for_offers(self):
        count = 0
        result = False
        while not self.conn.framework_id and count < 10:
            sleep(3)
            print('.')
            count += 1

        if not self.conn.framework_id:
            print("Failed to register, terminating Framework")
            self.background_thread.close_channel()
        else:
            count = 0
            while not self.conn.offers and count < 10:
                print('.')
                sleep(3)
                count += 1

            if not self.conn.offers:
                print("Failed to obtain resources, terminating Framework")
                self.conn.terminate_framework(self.conn.framework_id)
                self.background_thread.close_channel()
            else:
                print("Got offers:")
                pretty.pprint(self.conn.offers)
                result = True
        return result

    def launch(self):
        for i in range(0, len(self.conn.offers)):
            print("Starting offer ", i)
            offer = self.conn.offers.get('offers')[i]
            launch_json = get_json(LAUNCH_JSON)

            task_id = str(random.randint(100, 1000))

            launch_json["accept"]["offer_ids"].append(offer["id"])
            launch_json["framework_id"]["value"] = self.conn.framework_id

            task_infos = launch_json["accept"]["operations"][0]["launch"]["task_infos"][0]

            task_infos["task_id"]["value"] = task_id
            task_infos["command"]["value"] = "cd /var/local/www && /usr/bin/python -m SimpleHTTPServer 9000"
            task_infos["agent_id"]["value"] = self.conn.offers.get('offers')[0]["agent_id"]["value"]
            task_infos["resources"] = get_json(TASK_RESOURCES_JSON)

            q = Queue(1)
            self.framework.request("launch", launch_json, q)
            r = q.get()
            print("reply: {}".format(r))


def main():
    launcher = Launcher("http://192.168.33.10:5050")
    launcher.connect()
    if launcher.wait_for_offers():
        launcher.launch()
    launcher.background_thread.join()

if __name__ == '__main__':
    main()
