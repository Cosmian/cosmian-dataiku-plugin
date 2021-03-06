
from dataiku.core.intercom import backend_api_get_call

class ExportModelChartsAndTemplates(object):
    """
    Will be used on MDG to download puppeteer's result from the backend
    charts : seq of tuples (chartId, cssSelector, url)
    """
    def __init__(self, project_key, export_id):
        self.project_key = project_key
        self.export_id = export_id
        self.future = None
        self.future_id = None
        self.result = None
        self.fail_fatal = None
        self.file_contents = None

    def download(self):
        file = backend_api_get_call("export/model/charts/download?projectKey=" + self.project_key + "&exportId=" + self.export_id, None)
        print("FILE__EXPORTED:", file, "Class:", str(file.__class__))
        self.file_contents = file.content
        return file.headers.get('content-type'), self.file_contents
