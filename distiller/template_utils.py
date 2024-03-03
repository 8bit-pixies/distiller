from mako.template import Template
from pathlib import Path


class TemplateUtils:
    root_path = Path(__file__).parent.joinpath("templates")

    @classmethod
    def render_template(cls, filename, **kwargs):
        return Template(cls.root_path.joinpath(filename).open("r").read()).render(**kwargs)
