# coding=utf8
from setuptools import find_packages
from setuptools import setup
import os

version = '0.0.1'

setup(
    name='collective.zodbdebug',
    version=version,
    description='Tools to debug a ZODB.',
    long_description=(
        open('README.rst').read() + '\n\n' +
        open(os.path.join('docs', 'HISTORY.rst')).read()
    ),
    # Get more strings from
    # http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Framework :: ZODB',
        'Framework :: Zope :: 2',
        'Framework :: Zope',
        'Framework :: Zope2',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python',
    ],
    keywords='zope zodb debug',
    author='Rafael Oliveira',
    author_email='rafaelbco@gmail.com',
    url='https://github.com/collective/collective.zodbdebug',
    license='GPL',
    packages=find_packages(exclude=['ez_setup']),
    namespace_packages=['collective'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'ZODB3',
        'docopt',
        'plone.memoize',
        'rbco.caseclasses',
        'setuptools',
        'walkdir',
    ],
    entry_points={
        'zopectl.command': [
            'scan_blobs = collective.zodbdebug.scripts.scan_blobs:main',
            'show_transactions = collective.zodbdebug.scripts.show_transactions:main',
        ]
    },
)
