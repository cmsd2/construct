import itertools
from Queue import Queue
from threading import Thread

framework_req_id = itertools.count()

class FrameworkMessage:
    def __init__(self, kind, value):
        self.kind = kind
        self.value = value

    def __str__(self):
        return "msg(kind: {}, value: {})".format(self.kind, self.value)

class FrameworkRequest:
    def __init__(self, kind, body, reply_channel=None):
        global framework_req_id
        self.id = framework_req_id.next()
        self.kind = kind
        self.body = body
        self.reply_channel = reply_channel

    def __str__(self):
        return "request(id: {}, body: {})".format(self.id, self.body)

class Framework(Thread):
    def __init__(self, connector, api_url):
        super(Framework, self).__init__()
        self.api_url = api_url
        self.cancelled = False
        self.queue = Queue()
        self.connector = connector
        self.connector_thread = connector.register_framework(self.queue)
        self.pending = []
        self.daemon = True
        self.inflight = {}
        self.max_inflight = 1
        self.agent_tasks = {}

    def run(self):
        while not self.cancelled:
            item = self.dequeue_message()
            print("processing message {}".format(item))
            kind = item.kind
            value = item.value
            if kind == "cancel":
                self.cancelled = True
            elif kind == "response":
                self.handle_response(value)
            elif kind == "request":
                self.handle_request(value)
            elif kind == "offers":
                self.handle_offers(value)
            elif kind == "update":
                self.handle_update(value)
            elif kind == "error":
                pass
            elif kind == "subscribed":
                pass
            elif kind == "heartbeat":
                pass
            else:
                print("unknown message type {}".format(kind))
        print("framework thread exiting")

    def dequeue_message(self):
        req_msg = None
        if len(self.inflight) < self.max_inflight and len(self.pending) > 0:
            req_msg = self.pending.pop(0)
        else:
            req_msg = self.queue.get()
        return req_msg
        
    def cancel(self):
        self.queue.put(FrameworkMessage("cancel", None))

    def response(self, resp):
        self.queue.put(FrameworkMessage("response", resp))

    def request(self, kind, req, reply_channel=None):
        req_msg = FrameworkRequest(kind, req, reply_channel)
        self.queue.put(FrameworkMessage("request", req_msg))

    def handle_offers(self, req_msg):
        offers = req_msg["offers"]["offers"]
        for offer in offers:
            agent = self.add_offer(offer)

    def add_offer(self, offer):
        agent_id = offer["agent_id"]["value"]
        agent = self.get_or_create_agent(agent_id)
        agent["offer"] = offer
        print("agent status: {}".format(agent))
        return agent

    def handle_update(self, req_msg):
        agent_id = req_msg["update"]["status"]["agent_id"]["value"]
        self.add_task(agent_id, req_msg)

    def add_task(self, agent_id, req_msg):
        agent = self.get_or_create_agent(agent_id)
        agent["task"] = req_msg
        print("agent status: {}".format(agent))
        
    def get_or_create_agent(self, agent_id):
        agent = {}
        if agent_id not in self.agent_tasks:
            self.agent_tasks[agent_id] = agent
        else:
            agent = self.agent_tasks[agent_id]
        return agent
    
    def handle_request(self, req_msg):
        print("request {}".format(req_msg))
        if len(self.inflight) == self.max_inflight:
            print("{} requests already in flight. enqueing to pending".format(self.max_inflight))
            self.pending.append(FrameworkMessage("request", req_msg))
        else:
            print("dispatching request now")
            self.exec_request(req_msg)

    def handle_response(self, resp):
        print("response {}".format(resp))

    def exec_request(self, req_msg):
        try:
            if req_msg.kind == "launch":
                self.post_async(req_msg)                
            else:
                print("unsupported request type {}".format(req_msg.kind))
        except ValueError, err:
            print("Request failed: {}".format(err))
            if req_msg.reply_channel is not None:
                req_msg.reply_channel.put(err)
                

    def post_async(self, req_msg):
        r = self.post(req_msg)
        if 200 <= r.status_code < 300:
            self.inflight[req_msg.id] = req_msg
        elif req_msg.reply_channel is not None:
            print("returning failed async request result")
            req_msg.put(r)
        else:
            print("no reply channel for async request in flight")
        return r

    def post(self, req_msg):
        r = self.connector.post(self.api_url, req_msg.body, self.queue)
        print("Result: {}".format(r.status_code))
        if r.text:
            print(r.text)
        if 200 <= r.status_code < 300:
            print("Successfully posted")
        return r
        

