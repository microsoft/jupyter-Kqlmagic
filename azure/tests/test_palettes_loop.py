#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
#--------------------------------------------------------------------------

""" Test for current default color palette. """
import pytest
from Kqlmagic.constants import Constants
from Kqlmagic.kql_magic import Kqlmagic as Magic
from bs4 import BeautifulSoup
import os.path
import re
import tempfile

ip = get_ipython() # pylint: disable=E0602

@pytest.fixture 
def register_magic():
    magic = Magic(shell=ip)
    ip.register_magics(magic)

TEST_URI_SCHEMA_NAME = "kusto"

def test_ok(register_magic):
    assert True

# more palettes to be tested can be added to the dict
palette_dicts = {'tab10': {'commands': {'name': 'tab10', 'colors': 16, 'desaturation': 0.5}, 
                            'results': {'title': '<div>tab10 (16 colors, desaturation 0.5)</div>', 'rgb_values':    
                                                                                ['rgb(68, 112, 142)', 'rgb(194, 130, 74)', 
                                                                                'rgb(73, 131, 73)', 'rgb(170, 82, 83)', 
                                                                                'rgb(147, 124, 167)', 'rgb(123, 96, 91)', 
                                                                                'rgb(199, 146, 183)', 'rgb(127, 127, 127)', 
                                                                                'rgb(149, 150, 72)', 'rgb(69, 152, 161)', 
                                                                                'rgb(68, 112, 142)', 'rgb(194, 130, 74)', 
                                                                                'rgb(73, 131, 73)', 'rgb(170, 82, 83)', 
                                                                                'rgb(147, 124, 167)', 'rgb(123, 96, 91)'
                                                                                ]}
                },
                'pastel': {'commands': {'name': 'pastel', 'colors': 8, 'desaturation': 0.8}, 
                            'results': {'title': '<div>pastel (8 colors, desaturation 0.8)</div>', 'rgb_values': 
                                                                                ['rgb(169, 201, 235)', 'rgb(242, 182, 142)', 
                                                                                'rgb(149, 220, 165)', 'rgb(245, 168, 165)', 
                                                                                'rgb(210, 193, 248)', 'rgb(215, 187, 161)', 
                                                                                'rgb(242, 183, 225)', 'rgb(207, 207, 207)'
                                                                                ]}
                },
                'BrBG': {'commands': {'name': 'BrBG', 'colors': 12, 'desaturation': 0.65}, 
                            'results': {'title': '<div>BrBG (12 colors, desaturation 0.65)</div>', 'rgb_values': 
                                                                                 ['rgb(105, 70, 29)', 'rgb(142, 103, 52)', 
                                                                                'rgb(178, 144, 93)', 'rgb(207, 189, 145)', 
                                                                                'rgb(232, 221, 194)', 'rgb(242, 238, 229)', 
                                                                                'rgb(230, 238, 237)', 'rgb(195, 222, 218)', 
                                                                                'rgb(145, 193, 186)', 'rgb(92, 151, 145)', 
                                                                                'rgb(45, 110, 105)', 'rgb(16, 75, 69)'
                                                                                ]}
                }
}



#TEST IF EVERY PALETTE IN PALETTE_DICT IS CONFIGURED CORRECTLY 
def test_show_default_palette(register_magic):
    # iterate over palettes in dict to test for correct output
    for palette in palette_dicts.values():
        palette_dict_loop(register_magic,palette)


def palette_dict_loop(register_magic, palette_dict):
    # iterate over Kqlmgic palette config commands and set defaults
    for command in palette_dict['commands']:
        if command == 'name':           # if Kqlmagic.palette_name: value is a string
            ip.run_line_magic('config', "Kqlmagic.palette_{0} = '{1}'".format(command, palette_dict['commands'][command]))
        else:                           
            ip.run_line_magic('config', "Kqlmagic.palette_{0} = {1}".format(command, palette_dict['commands'][command]))
    # check for correct output
    command = '--palette'
    html_result = ip.run_line_magic('kql', command)._repr_html_()
    soup = BeautifulSoup(html_result, 'lxml')
    divs = soup.div.contents 
    rgb_results = []      
    for div in divs[1:]:
        for style in div.attrs:
            r = re.search(r'color\:(.+?)\;height', div.attrs[style])
            rgb = r.group(1)
            rgb_results.append(rgb)
    assert palette_dict['results']['title'] == str(divs[0])
    assert palette_dict['results']['rgb_values'] == rgb_results 