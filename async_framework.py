import itertools
from Queue import Queue

framework_req_id = itertools.count()

class FrameworkMessage:
    def __init__(self, kind, value):
        self.kind = kind
        self.value = value

class FrameworkRequest:
    def __init__(self, body):
        global framework_req_id
        self.id = framework_req_id.next()
        self.body = body

    def __str__(self):
        return "request(id: {}, body: {})".format(self.id, self.body)

class Framework(Thread):
    def __init__(self, connector):
        super(Framework, self).__init__()
        self.cancelled = False
        self.connector = connector
        self.connector_thread = connector.register_framework()
        self.queue = Queue()
        self.pending = []
        self.daemon = True
        self.inflight = {}
        self.max_inflight = 1

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

    def request(self, req):
        req_msg = FrameworkRequest(req)
        self.queue.put(FrameworkMessage("request", req_msg))

    def handle_request(self, req_msg):
        print("request {}".format(req_msg))
        if len(self.inflight) == self.max_inflight:
            self.pending.append(FrameworkMessage("request", req_msg))
        else:
            self.exec_request(req_msg)

    def handle_response(self, resp):
        print("response {}".format(resp))

    def exec_request(self, req_msg):
        self.inflight[req_msg.id] = req_msg
        pass
