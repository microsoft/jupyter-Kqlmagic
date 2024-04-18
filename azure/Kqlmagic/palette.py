# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import colorsys
from itertools import cycle
import re
from typing import List


from .dependencies import Dependencies
from .my_utils import is_collection


class Color(object):

    def __init__(self, rgb_color, name=None, **kwargs):
        self.color = rgb_color
        self.name = name or rgb_color


    def _repr_html_(self):
        return self._to_html()


    def _to_html(self):
        c = f'<div style="background-color:{self.color};height:20px;width:20px;display:inline-block;"></div>'
        return f'<div style="display:inline-block;padding:10px;"><div>{self.name}</div>{c}</div>'


    def __repr__(self):
        return self.color


class Palette(list):

    def __init__(self, palette_name=None, n_colors=None, desaturation=None, rgb_palette=None, range_start=None, to_reverse=False, **kwargs):
        self.name = palette_name or Palettes.get_default_pallete_name()
        self.n_colors = (n_colors or Palettes.DEFAULT_N_COLORS) if rgb_palette is None else len(rgb_palette)
        self.desaturation = desaturation or Palettes.DEFAULT_DESATURATION
        self.kwargs = kwargs
        self.range_start = range_start

        parsed = self.parse(self.name)
        self.name = parsed.get("name") or self.name

        if rgb_palette is None:
            rgb_palette = parsed.get("rgb_palette")
            if rgb_palette is None:
                rgb_float_pallete = self._get_color_palette(name=parsed.get("base_name"), n_colors=self.n_colors, desaturation=self.desaturation)
                rgb_palette = ["rgb" + str((int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255))) for rgb in rgb_float_pallete]
            if parsed.get("slice"):
                rgb_palette = rgb_palette.__getitem__(parsed.get("slice"))
            if parsed.get("reversed") is not None and parsed.get("reversed"):
                rgb_palette = list(reversed(rgb_palette))
            if to_reverse:
                rgb_palette = list(reversed(rgb_palette))
        super(Palette, self).__init__()
        self.extend(rgb_palette)


    def _to_html(self, add_details_to_name=True):
        name = self.name
        if self.range_start is not None:
            name = f"{name}[{self.range_start}:{self.range_start + len(self)}]"
        if add_details_to_name:
            desaturation_details = ""
            if self.desaturation is not None and self.desaturation > 0 and self.desaturation < 1.0:
                desaturation_details = f", desaturation {self.desaturation}"
            name = f"{name} ({self.n_colors} colors{desaturation_details})"
        s_s = ""
        for color in self:
            s_s = f'{s_s}<div style="background-color:{color};height:20px;width:20px;display:inline-block;"></div>'
            
        return f'<div style="display:inline-block;padding:10px;"><div>{name}</div>{s_s}</div>'


    def __getitem__(self, key):
        item = super(Palette, self).__getitem__(key)
        if isinstance(key, slice):
            range_start = min((key.start or 0), len(self)) + (self.range_start or 0)
            return Palette(palette_name=self.name, desaturation=self.desaturation, rgb_palette=item, range_start=range_start, **self.kwargs)
        else:
            return Color(item, name=f"{self.name}[{(self.range_start or 0) + key}]")


    def _repr_html_(self):
        return self._to_html()


    @classmethod
    def parse(cls, name):
        name = name.strip()
        reverse = name.endswith("_r")
        base_name = name[:-2] if reverse else name

        rgb_palette = None
        range = None

        if base_name.endswith("]"):
            start = base_name.rfind("[")
            # slice
            if start > 0:
                se_parts = [value.strip() for value in base_name[start + 1: -1].split(":")]
                if len(se_parts) == 2:
                    try:
                        range = slice(*[int(value) if value else None for value in se_parts])
                        base_name = base_name[:start]
                    except: # pylint: disable=bare-except
                        pass

        # custom
        if is_collection(base_name, "["):
            rgb_palette = eval(base_name.lower().replace("-", "").replace("_", ""))
            if not isinstance(rgb_palette, list) or len(rgb_palette) == 0:
                raise ValueError("invlaid custom palette syntax, should be a comma separate list.'[\"rgb(r,g,b)\",...]'")

            for rgb in rgb_palette:
                if not isinstance(rgb, str) or not rgb.startswith("rgb"):
                    raise ValueError("invlaid custom palette syntax, each item must have a 'rgb' prefix.'[\"rgb(r,g,b)\",\"rgb(...)\",...]'")
                color_list = eval(rgb[3:])
                if len(color_list) != 3:
                    raise ValueError("invlaid custom palette syntax, each color must be composed of a list of 3 number: \"rgb(r,g,b)\"")

                for color in color_list:
                    if not isinstance(color, int) or color < 0 or color > 255:
                        raise ValueError("invlaid custom palette syntax, each basic color (r,g,b) must between 0 to 255")
            name = name.lower().replace("-", "").replace("_", "").replace(" ", "")
            base_name = None

        return {"name": name, "base_name": base_name, "rgb_palette": rgb_palette, "reversed": reverse, "slice": range}


    @classmethod
    def validate_palette_name(cls, name):
        parsed = cls.parse(name)
        if parsed.get("rgb_palette") is None and parsed.get("base_name") not in Palettes.get_all_pallete_names():
            raise ValueError(
                f"must be a known palette name or custom palette (see option -popup_palettes) , but a value of {name} was specified."
            )


    @classmethod
    def validate_palette_desaturation(cls, desaturation):
        if desaturation > 1 or desaturation < 0:
            raise ValueError(f"must be between 0 and 1, but a value of {str(desaturation)} was specified.")


    @classmethod
    def validate_palette_colors(cls, n_colors):
        if n_colors < 1:
            raise ValueError(f"must be greater or equal than 1, but a value of {str(n_colors)} was specified.")

    @classmethod
    def _get_color_palette(cls, name=None, n_colors=1, desaturation=1):
        if name in Palettes.DEFAULT_PALETTES:       
            default_palette = Palettes.DEFAULT_PALETTES[name]
            pal_cycle = cycle(default_palette)
            palette = [next(pal_cycle) for _ in range(n_colors)]
            rgb_float_pallete = list(map(cls._rrggbb_to_rgb, palette))
            
        else:
            mplcmap = Dependencies.get_module('matplotlib.cm')
            mplcol = Dependencies.get_module('matplotlib.colors')
            
            if name in Palettes.MATPLOTLIB_DISTINCT_PALETTES:
                num = Palettes.MATPLOTLIB_DISTINCT_PALETTES[name]
                vector = [idx / (num - 1) for idx in range(0,num)][:n_colors]
            else:
                num = int(n_colors) + 2
                vector = [idx / (num - 1) for idx in range(0,num)][1:-1]

            color_map = mplcmap.get_cmap(name)
            palette = map(tuple, color_map(vector)[:, :3])

            rgb_float_pallete = list(map(mplcol.colorConverter.to_rgb, palette))

        rgb_float_pallete_desaturated = cls._desaturate_palette(rgb_float_pallete, desaturation)
        return rgb_float_pallete_desaturated


    @classmethod
    def _rrggbb_to_rgb(cls, hex_color):
        """Convert color in hex format #rrggbb or #rgb to an RGB color."""
        if isinstance(hex_color, str):
            # hex color in #rrggbb format.
            match = re.match(r"\A#[a-fA-F0-9]{6}\Z", hex_color)
            if match:
                return (tuple(int(value, 16) / 255 for value in [hex_color[1:3], hex_color[3:5], hex_color[5:7]]))
            # hex color in #rgb format, shorthand for #rrggbb.
            match = re.match(r"\A#[a-fA-F0-9]{3}\Z", hex_color)
            if match:
                return (tuple(int(value, 16) / 255 for value in [hex_color[1] * 2, hex_color[2] * 2, hex_color[3] * 2]))
        return hex_color


    @classmethod
    def _desaturate_palette(cls, reg_float_palette, desaturation):
        if not 0 <= desaturation <= 1:
            return [*reg_float_palette]

        return [cls._desaturate_rgb(rgb, desaturation) for rgb in reg_float_palette]


    @classmethod
    def _desaturate_rgb(cls, rgb, desaturation):
        if not 0 <= desaturation <= 1:
            return [*rgb]

        hue, lightness, saturation = colorsys.rgb_to_hls(*rgb)
        saturation *= desaturation
        saturated_rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
        return saturated_rgb


class Palettes(list):

            # mplcmap = Dependencies.get_module('matplotlib.cm')
            # mplcol = Dependencies.get_module('matplotlib.colors')
    DEFAULT_DESATURATION = 1.0
    DEFAULT_N_COLORS = 10
    DEFAULT_NAME = "tab10" # should be from BASE_PALETTE_NAMES
    DEFAULT_ALT_NAME = "pastel" # should be from DEFAULT_PALETTES


    # DEFAULT_PALETTES: old matplotlib default palette
    DEFAULT_PALETTES = dict(
        deep=["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3",
              "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD"],
        deep6=["#4C72B0", "#55A868", "#C44E52",
               "#8172B3", "#CCB974", "#64B5CD"],
        muted=["#4878D0", "#EE854A", "#6ACC64", "#D65F5F", "#956CB4",
               "#8C613C", "#DC7EC0", "#797979", "#D5BB67", "#82C6E2"],
        muted6=["#4878D0", "#6ACC64", "#D65F5F",
                "#956CB4", "#D5BB67", "#82C6E2"],
        pastel=["#A1C9F4", "#FFB482", "#8DE5A1", "#FF9F9B", "#D0BBFF",
                "#DEBB9B", "#FAB0E4", "#CFCFCF", "#FFFEA3", "#B9F2F0"],
        pastel6=["#A1C9F4", "#8DE5A1", "#FF9F9B",
                 "#D0BBFF", "#FFFEA3", "#B9F2F0"],
        bright=["#023EFF", "#FF7C00", "#1AC938", "#E8000B", "#8B2BE2",
                "#9F4800", "#F14CC1", "#A3A3A3", "#FFC400", "#00D7FF"],
        bright6=["#023EFF", "#1AC938", "#E8000B",
                 "#8B2BE2", "#FFC400", "#00D7FF"],
        dark=["#001C7F", "#B1400D", "#12711C", "#8C0800", "#591E71",
              "#592F0D", "#A23582", "#3C3C3C", "#B8850A", "#006374"],
        dark6=["#001C7F", "#12711C", "#8C0800",
               "#591E71", "#B8850A", "#006374"],
        colorblind=["#0173B2", "#DE8F05", "#029E73", "#D55E00", "#CC78BC",
                    "#CA9161", "#FBAFE4", "#949494", "#ECE133", "#56B4E9"],
        colorblind6=["#0173B2", "#029E73", "#D55E00",
                     "#CC78BC", "#ECE133", "#56B4E9"]
    )

    # matplotlib clormap + DEFAULT_PALETTES
    BASE_PALETTE_NAMES = [
        "deep",
        "muted",
        "bright",
        "pastel",
        "dark",
        "colorblind",

        "Accent",
        "Blues",
        "BrBG",
        "BuGn",
        "BuPu",
        "CMRmap",
        "Dark2",
        "GnBu",
        "Greens",
        "Greys",
        "OrRd",
        "Oranges",
        "PRGn",
        "Paired",
        "Pastel1",
        "Pastel2",
        "PiYG",
        "PuBu",
        "PuBuGn",
        "PuOr",
        "PuRd",
        "Purples",
        "RdBu",
        "RdGy",
        "RdPu",
        "RdYlBu",
        "RdYlGn",
        "Reds",
        "Set1",
        "Set2",
        "Set3",
        "Spectral",
        "Wistia",
        "YlGn",
        "YlGnBu",
        "YlOrBr",
        "YlOrRd",
        "afmhot",
        "autumn",
        "binary",
        "bone",
        "brg",
        "bwr",
        "cividis",
        "cool",
        "coolwarm",
        "copper",
        "cubehelix",
        "flag",
        "gist_earth",
        "gist_gray",
        "gist_heat",
        "gist_ncar",
        "gist_rainbow",
        "gist_stern",
        "gist_yarg",
        "gnuplot",
        "gnuplot2",
        "gray",
        "hot",
        "hsv",
        "inferno",
        "magma",
        "nipy_spectral",
        "ocean",
        "pink",
        "plasma",
        "prism",
        "rainbow",
        "seismic",
        "spring",
        "summer",
        "tab10",
        "tab20",
        "tab20b",
        "tab20c",
        "terrain",
        "viridis",
        "winter",
    ]

    MATPLOTLIB_DISTINCT_PALETTES = {
        "tab10": 10, "tab20": 20, "tab20b": 20, "tab20c": 20,
        "Set1": 9, "Set2": 8, "Set3": 12,
        "Accent": 8, "Paired": 12,
        "Pastel1": 9, "Pastel2": 8, "Dark2": 8,
    }

    all_palette_names = []
    default_palette_name = None

    @classmethod
    def get_all_pallete_names(cls)->List[str]:
        if len(cls.all_palette_names) == 0:
            mplcmap = Dependencies.get_module('matplotlib.cm', dont_throw=True)
            mplcol = Dependencies.get_module('matplotlib.colors', dont_throw=True)
            if mplcmap and mplcol:
                cls.all_palette_names = cls.BASE_PALETTE_NAMES
            else:
                cls.all_palette_names = list(cls.DEFAULT_PALETTES.keys())
        return cls.all_palette_names


    @classmethod
    def get_default_pallete_name(cls)->str:
        if cls.default_palette_name is None:
            if cls.DEFAULT_NAME in cls.get_all_pallete_names():
                cls.default_palette_name = cls.DEFAULT_NAME
            else:
                cls.default_palette_name = cls.DEFAULT_ALT_NAME
        return cls.default_palette_name


    def __init__(self, n_colors=None, desaturation=None, palette_list=None, to_reverse=False, **kwargs):
        self.n_colors = n_colors or Palettes.DEFAULT_N_COLORS
        self.desaturation = desaturation or Palettes.DEFAULT_DESATURATION
        self.to_reverse = to_reverse
        self.kwargs = kwargs
        super(Palettes, self).__init__()
        self.extend(palette_list or Palettes.get_all_pallete_names())


    def __getitem__(self, key):
        if isinstance(key, str):
            key = self.index(key)
        item = super(Palettes, self).__getitem__(key)
        if isinstance(key, slice):
            return Palettes(palette_list=item, desaturation=self.desaturation, n_colors=self.n_colors, to_reverse=self.to_reverse, **self.kwargs)
        else:
            return Palette(palette_name=item, desaturation=self.desaturation, n_colors=self.n_colors, to_reverse=self.to_reverse, **self.kwargs)


    def _to_html(self):
        n_colors = self.n_colors
        desaturation = self.desaturation
        suffix = f" (desaturation {str(desaturation)})" if desaturation is not None and desaturation != 1.0 and desaturation != 0 else ""
        html_str = f'<div style="text-align:center"><h1>{n_colors} colors palettes{suffix}</h1></div>'
        for name in self:
            for suffix in [""]:  # ['', '_r']:
                s = Palette(palette_name=name + suffix, n_colors=n_colors, desaturation=desaturation, **self.kwargs)
                html_str += s._to_html(add_details_to_name=False)
        return html_str


    def _repr_html_(self):
        return self._to_html()


    # plotly support this css colors:

"""       - A named CSS color:
            aliceblue, antiquewhite, aqua, aquamarine, azure,
            beige, bisque, black, blanchedalmond, blue,
            blueviolet, brown, burlywood, cadetblue,
            chartreuse, chocolate, coral, cornflowerblue,
            cornsilk, crimson, cyan, darkblue, darkcyan,
            darkgoldenrod, darkgray, darkgrey, darkgreen,
            darkkhaki, darkmagenta, darkolivegreen, darkorange,
            darkorchid, darkred, darksalmon, darkseagreen,
            darkslateblue, darkslategray, darkslategrey,
            darkturquoise, darkviolet, deeppink, deepskyblue,
            dimgray, dimgrey, dodgerblue, firebrick,
            floralwhite, forestgreen, fuchsia, gainsboro,
            ghostwhite, gold, goldenrod, gray, grey, green,
            greenyellow, honeydew, hotpink, indianred, indigo,
            ivory, khaki, lavender, lavenderblush, lawngreen,
            lemonchiffon, lightblue, lightcoral, lightcyan,
            lightgoldenrodyellow, lightgray, lightgrey,
            lightgreen, lightpink, lightsalmon, lightseagreen,
            lightskyblue, lightslategray, lightslategrey,
            lightsteelblue, lightyellow, lime, limegreen,
            linen, magenta, maroon, mediumaquamarine,
            mediumblue, mediumorchid, mediumpurple,
            mediumseagreen, mediumslateblue, mediumspringgreen,
            mediumturquoise, mediumvioletred, midnightblue,
            mintcream, mistyrose, moccasin, navajowhite, navy,
            oldlace, olive, olivedrab, orange, orangered,
            orchid, palegoldenrod, palegreen, paleturquoise,
            palevioletred, papayawhip, peachpuff, peru, pink,
            plum, powderblue, purple, red, rosybrown,
            royalblue, saddlebrown, salmon, sandybrown,
            seagreen, seashell, sienna, silver, skyblue,
            slateblue, slategray, slategrey, snow, springgreen,
            steelblue, tan, teal, thistle, tomato, turquoise,
            violet, wheat, white, whitesmoke, yellow,
            yellowgreen
            """

# Have colormaps separated into categories:
# http://matplotlib.org/examples/color/colormaps_reference.html
# cmaps = [('Perceptually Uniform Sequential', [
#             'viridis', 'plasma', 'inferno', 'magma']),
#          ('Sequential', [
#             'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds',
#             'YlOrBr', 'YlOrRd', 'OrRd', 'PuRd', 'RdPu', 'BuPu',
#             'GnBu', 'PuBu', 'YlGnBu', 'PuBuGn', 'BuGn', 'YlGn']),
#          ('Sequential (2)', [
#             'binary', 'gist_yarg', 'gist_gray', 'gray', 'bone', 'pink',
#             'spring', 'summer', 'autumn', 'winter', 'cool', 'Wistia',
#             'hot', 'afmhot', 'gist_heat', 'copper']),
#          ('Diverging', [
#             'PiYG', 'PRGn', 'BrBG', 'PuOr', 'RdGy', 'RdBu',
#             'RdYlBu', 'RdYlGn', 'Spectral', 'coolwarm', 'bwr', 'seismic']),
#          ('Qualitative', [
#             'Pastel1', 'Pastel2', 'Paired', 'Accent',
#             'Dark2', 'Set1', 'Set2', 'Set3',
#             'tab10', 'tab20', 'tab20b', 'tab20c']),
#          ('Miscellaneous', [
#             'flag', 'prism', 'ocean', 'gist_earth', 'terrain', 'gist_stern',
#             'gnuplot', 'gnuplot2', 'CMRmap', 'cubehelix', 'brg', 'hsv',
#             'gist_rainbow', 'rainbow', 'jet', 'nipy_spectral', 'gist_ncar'])]
