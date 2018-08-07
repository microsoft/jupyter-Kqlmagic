import time
from IPython.core.display import display, HTML
from IPython.core.magics.display import Javascript





class Help_html(object):
    """
    """

    @staticmethod
    def add_menu_item(text, url, **kwargs):
        # add help link
        help_links = get_ipython().kernel._trait_values['help_links']
        found = False
        for link in help_links:
            # if found update url
            if link.get('text') == text:
                link['url'] = url
                found = True
                break
        if not found:
            help_links.append({'text': text, 'url': url})
        display(Javascript("""IPython.notebook.kernel.reconnect();"""))
        time.sleep(1)


