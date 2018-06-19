from setuptools import setup, find_packages
from pkg_resources import Requirement, resource_filename
filename = resource_filename(Requirement.parse("mesosrc"), "mesosrc.conf")

setup(
    name="mesosrc",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,

    install_requires=['cement>=2.10.2',
                      'colorlog>=3.1.4',
                      'tabulate>=0.8.2'
                      ],
    entry_points={
        'console_scripts': [
            'mesosrc=mesosrc.mesosrc:main',
        ],
    },

    data_files=[
        ('etc/mesosrc', ['mesosrc/mesosrc.conf.example'])
    ],

    license='MIT',
    author="Eugene Paniot",
    author_email="Eugene.Paniot@nordigy.ru",
    description="Mesos RingCentral CLI",
    project_urls={
        "Source Code": "https://git.ringcentral.com/opstools/mesosrc-cli",
    }
)
# build with python setup.py bdist_egg