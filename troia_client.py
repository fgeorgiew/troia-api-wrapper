import json
import requests

class TroiaClient(object):

    __ENDPOINT_STATUS = 'status'
    __ENDPOINT_ADD_JOB = 'jobs'
    __ENDPOINT_DELETE_JOB = 'jobs'
    __ENDPOINT_GET_JOB = 'jobs/{}'
    __ENDPOINT_COMPUTE = 'jobs/{}/compute'
    __ENDPOINT_ADD_ASSIGNS = 'jobs/{}/assigns'
    __ENDPOINT_GET_WORKER = 'jobs/{}/workers/{}/info'
    __ENDPOINT_GET_WORKERS = 'jobs/{}/workers'
    __ENDPOINT_GET_WORKERS_COST = 'jobs/{}/workers/cost/estimated/'
    __ENDPOINT_GET_WORKERS_QUALITY_MATRIX = 'jobs/{}/workers/quality/matrix/'
    __ENDPOINT_GET_WORKERS_PAYMENT = 'jobs/{}/workers/quality/payment/'
    __ENDPOINT_GET_WORKERS_QUALITY_ESTIMATED = 'jobs/{}/workers/quality/estimated/'
    __ENDPOINT_GET_WORKERS_QUALITY_EVALUATED = 'jobs/{}/workers/quality/evaluated/'
    __ENDPOINT_GET_WORKERS_QUALITY_SUMMARY = 'jobs/{}/workers/quality/summary'
    __ENDPOINT_GET_OBJECTS_PREDICTIONS = 'jobs/{}/objects/prediction'

    def __init__(self, base_url, timeout=0.2):
        self.base_url = base_url
        if not self.base_url.endswith('/'):
            self.base_url += '/'

        if timeout <= 0.:
            self.timeout = None
        else:
            self.timeout = timeout

    def __http_get(self, endpoint, args=None):
        resp = requests.get(self.base_url + endpoint, params=args, timeout=self.timeout)
        return resp

    def __http_post(self, endpoint, args=None):
        resp = requests.post(self.base_url + endpoint, data=args, timeout=self.timeout)
        return resp

    def __http_delete(self, endpoint, args=None):
        resp = requests.delete(self.base_url + endpoint, data=args, timeout=self.timeout)
        return resp

    def get_status(self):
        resp = self.__http_get(self.__ENDPOINT_STATUS)
        return TroiaResponse(resp)

    def add_job(self, params=None):
        resp = self.__http_post(self.__ENDPOINT_ADD_JOB, params)
        return TroiaResponse(resp)

    def get_job(self, job_id):
        resp = self.__http_get(self.__ENDPOINT_GET_JOB.format(job_id))
        return TroiaResponse(resp)

    def delete_job(self, job_id):
        params = json.JSONEncoder.encode({'id': job_id})
        resp = self.__http_delete(self.__ENDPOINT_DELETE_JOB, params)
        return TroiaResponse(resp)

    def compute(self, job_id):
        resp = self.__http_post(self.__ENDPOINT_COMPUTE.format(job_id))
        return TroiaResponse(resp)

    def add_assigns(self, job_id, params):
        resp = self.__http_post(self.__ENDPOINT_ADD_ASSIGNS.format(job_id), params)
        return TroiaResponse(resp)

    def get_worker(self, job_id, worker_id):
        resp = self.__http_get(self.__ENDPOINT_GET_WORKER.format(job_id,worker_id))
        return TroiaResponse(resp)

    def get_workers(self, job_id):
        resp = self.__http_get(self.__ENDPOINT_GET_WORKERS.format(job_id))
        return TroiaResponse(resp)

    def get_workers_cost(self, job_id):
        resp = self.__http_get(self.__ENDPOINT_GET_WORKERS_COST.format(job_id))
        return TroiaResponse(resp)

    def get_workers_quality_matrix(self, job_id):
        resp = self.__http_get(self.__ENDPOINT_GET_WORKERS_QUALITY_MATRIX.format(job_id))
        return TroiaResponse(resp)

    def get_workers_payment(self, job_id):
        resp = self.__http_get(self.__ENDPOINT_GET_WORKERS_PAYMENT.format(job_id))
        return TroiaResponse(resp)

    def get_workers_quality_estimated(self, job_id):
        resp = self.__http_get(self.__ENDPOINT_GET_WORKERS_QUALITY_ESTIMATED.format(job_id))
        return TroiaResponse(resp)

    def get_workers_quality_evaluated(self, job_id):
        resp = self.__http_get(self.__ENDPOINT_GET_WORKERS_QUALITY_EVALUATED.format(job_id))
        return TroiaResponse(resp)

    def get_workers_quality_summary(self, job_id):
        resp = self.__http_get(self.__ENDPOINT_GET_WORKERS_QUALITY_SUMMARY.format(job_id))
        return TroiaResponse(resp)

    def get_objects_predictions(self, job_id=None, label_choosing_method=1):

        if label_choosing_method == 1:
            label_choosing_method = 'MaxLikelihood'
        else:
            label_choosing_method = 'MinCost'

        params = {'labelChoosing': label_choosing_method}
        resp = self.__http_get(self.__ENDPOINT_GET_OBJECTS_PREDICTIONS.format(job_id), json.dumps(params))
        return TroiaResponse(resp)

    def get_response(self, redirect_url):
        resp = self.__http_get(redirect_url)
        return TroiaResponse(resp)

class TroiaResponse(object):

    def __init__(self, raw_response):
        self.raw_response = raw_response
        resp_content = self.get_content()
        self.status = resp_content['status'] if 'status' in resp_content else None
        self.timestamp = resp_content['timestamp'] if 'timestamp' in resp_content else None
        self.redirect = resp_content['redirect'] if 'redirect' in resp_content else None
        self.result = resp_content['result'] if 'result' in resp_content else None

    def get_json_content(self):
        return json.loads(self.raw_response.content)

    def get_content(self):
        return json.JSONDecoder().decode(self.raw_response.content)
