import sys
import json
import base64

from dataiku.base.utils import encode_utf8
from dataiku.core import dkuio
from dataiku import default_project_key
from dataiku.core.intercom import backend_void_call

__all__ = [
    'save_data',
    'save_figure',
    'save_bokeh',
    'save_plotly',
    'save_ggplot'
]

def _get_payload(payload, encoding=None):
    if encoding is None:
        # make sure we feed bytes to the base64 encoding
        payload = encode_utf8(payload)
        payload = base64.b64encode(payload)
    elif encoding != "base64":
        raise Exception('Invalid encoding: expected None or "base64"')
    return payload

def save_data(id, payload, content_type, label = None, project_key=None, encoding=None):
    """
    Saves data as a DSS static insight that can be exposed on the dashboard

    :param str id: Unique identifier of the insight within the project. If an insight with the same identifier
                   already exists, it will be replaced
    :param payload: bytes-oriented data, or Base64 string
    :param content_type: the MIME type of the data in payload (example: text/html or image/png)
    :param str label: Optional display label for the insight. If None, the id will be used as label
    :param str project_key: Project key in which the insight must be saved. If None, the contextual (current)
                    project is used
    :param str encoding: If the payload was a Base64 string, this must be "base64". Else, this must be None
    """
    if project_key is None:
        project_key = default_project_key()

    backend_void_call("insights/save-static-file-insight", {
        "projectKey": project_key,
        "id": id,
        "payload": _get_payload(payload, encoding),
        "contentType": content_type,
        "label": label
        
    })

# Matplotlib integration

def save_figure(id, figure = None, label=None, project_key=None):
    """
    Saves a matplotlib or seaborn figure as a DSS static insight that can be exposed on the dashboard

    :param str id: Unique identifier of the insight within the project. If an insight with the same identifier
                   already exists, it will be replaced
    :param figure: a matplotlib or seaborn figure object. If None, the latest processed figure will be used
    :param str label: Optional display label for the insight. If None, the id will be used as label
    :param str project_key: Project key in which the insight must be saved. If None, the contextual (current)
                    project is used
    """
    if figure is None:
        from matplotlib import pyplot
        figure = pyplot.gcf()
        if figure.get_axes() is None or len(figure.get_axes()) == 0:
            import warnings
            warnings.warn("Current figure is empty, save_figure() is not called from a cell where a plot is defined, or too early in the cell")
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    canvas = FigureCanvasAgg(figure)
    output = dkuio.new_bytesoriented_io()
    canvas.print_png(output)

    save_data(id, output.getvalue(), "image/png",
            label= label, project_key = project_key)

# Bokeh integration

def save_bokeh(id, bokeh_obj, label=None, project_key=None):
    """
    Saves a Bokeh object as a DSS static insight that can be exposed on the dashboard
    A static HTML export of the provided Bokeh object is done

    :param str id: Unique identifier of the insight within the project. If an insight with the same identifier
                   already exists, it will be replaced
    :param bokeh_obj: a Bokeh object
    :param str label: Optional display label for the insight. If None, the id will be used as label
    :param str project_key: Project key in which the insight must be saved. If None, the contextual (current)
                    project is used
    """
    from bokeh.embed import file_html
    import bokeh.resources
    html = file_html(bokeh_obj, bokeh.resources.INLINE, label)

    save_data(id, html, "text/html",
            label= label, project_key = project_key)

# Plot.ly integration

def save_plotly(id, figure, label=None, project_key=None):
    """
    Saves a Plot.ly figure as a DSS static insight that can be exposed on the dashboard
    A static HTML export of the provided Plot.ly object is done

    :param str id: Unique identifier of the insight within the project. If an insight with the same identifier
                   already exists, it will be replaced
    :param figure: a Plot.ly figure
    :param str label: Optional display label for the insight. If None, the id will be used as label
    :param str project_key: Project key in which the insight must be saved. If None, the contextual (current)
                    project is used
    """
    import plotly
    html = plotly.offline.plot(figure, output_type="div")
    save_data(id, html, "text/html",
            label= label, project_key = project_key)

# GGplot integration

def save_ggplot(id, gg, label=None, width=None, height=None, dpi=None, project_key = None):
    """
    Saves a ggplot object as a DSS static insight that can be exposed on the dashboard

    :param str id: Unique identifier of the insight within the project. If an insight with the same identifier
                   already exists, it will be replaced
    :param gg: a ggplot object
    :param str label: Optional display label for the insight. If None, the id will be used as label
    :param int width: Width of the image export
    :param int height: Height of the image export
    :param int dpi: Resolution in dots per inch of the image export
    :param str project_key: Project key in which the insight must be saved. If None, the contextual (current)
                    project is used
    """
    output = dkuio.new_bytesoriented_io()
    gg.save(output, width=width, height=height, dpi=dpi)
    save_data(id, output.getvalue(), "image/png",
            label= label, project_key = project_key)