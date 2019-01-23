#   Copyright 2010 Jacob Kaplan-Moss
#   Copyright 2011 OpenStack Foundation
#   Copyright 2011 Piston Cloud Computing, Inc.
#   Copyright 2013 David Moreau Simard
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#   Author: David Moreau Simard <moi@dmsimard.com>
#   Credit: python-novaclient
#

"""
A ceph-mgr/RESTful plugin interface that handles REST calls and responses.
"""

import logging
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import subprocess

try:
    from lxml import etree
except ImportError as e:
    print("Missing required python module: " + str(e))
    exit()

try:
    import json
except ImportError:
    import simplejson as json

import cephclient.exceptions as exceptions


class CephClient(object):

    def __init__(self, **params):
        """
        Initialize the class, get the necessary parameters
        """
        self.user_agent = 'python-cephclient'

        self.params = params
        self.log = self.log_wrapper()

        self.log.debug("Params: {0}".format(str(self.params)))

        if 'endpoint' in self.params:
            self.endpoint = self.params['endpoint']
        else:
            # default endpoint
            self.endpoint = 'https://localhost:5001/'

        if 'timeout' not in self.params:
            self.timeout = None

        self.http = requests.Session()

    def _restful_check_active(self):
        rst = subprocess.Popen("curl --connect-timeout 5 -k https://localhost:5001/ 2>&1", shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        return rst.wait()

    def _get_admin_key(self):
        if self._restful_check_active() != 0:
            raise Exception('RESTful plugin is not active. Please restart')
        p = subprocess.Popen("ceph restful list-keys --connect-timeout 5", shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        if p.wait() != 0:
            raise Exception('RESTful plugin: can not get list-keys. Please restart')
        rst = p.stdout.read()
        rst = json.loads(rst)
        if 'admin' not in rst.keys():
            raise Exception('admin user and its password not exist in RESTful plugin')
        admin_key = str(rst['admin'])
        return ("admin", admin_key)

    def _request(self, url, method, **kwargs):
        if self.timeout is not None:
            kwargs.setdefault('timeout', self.timeout)

        kwargs.setdefault('headers', kwargs.get('headers', {}))
        kwargs['headers']['User-Agent'] = self.user_agent

        try:
            if kwargs['body'] is 'json':
                kwargs['json']['format'] = 'json'
            elif kwargs['body'] is 'xml':
                kwargs['json']['format'] = 'xml'
            elif kwargs['body'] is 'text':
                kwargs['json']['format'] = 'plain'
            elif kwargs['body'] is 'binary':
                kwargs['headers']['Accept'] = 'application/octet-stream'
                kwargs['headers']['Content-Type'] = 'application/octet-stream'
            else:
                raise exceptions.UnsupportedRequestType()
        except KeyError:
            # Default if body type is unspecified is text/plain
            kwargs['json']['format'] = 'json'

        # Optionally verify if requested body type is supported
        try:
            if kwargs['body'] not in kwargs['supported_body_types']:
                raise exceptions.UnsupportedBodyType()
            else:
                del kwargs['supported_body_types']
        except KeyError:
            pass

        try:
            del kwargs['body']
        except KeyError:
            pass

        self.log.debug("{0} URL: {1}{2} - {3}"
                       .format(method, self.endpoint, url, str(kwargs)))

        resp = self.http.request(method, self.endpoint + url, auth = self._get_admin_key(), verify = False, **kwargs)
        rst = json.loads(resp.text)

        if rst['has_failed'] == True or rst['state'] != 'success' or len(rst['failed']) != 0 or rst['is_finished'] != True:
            resp.ok = False
            body = None
            return resp, body

        try:
            finished = rst['finished'][0]
            body = {}
            if (len(finished) == 0):
                body[u'status'] = u'ko'
            elif kwargs['json']['format'] is 'json':
                body[u'status'] = u'ok'
                body[u'output'] = json.loads(str(finished['outb'].strip('\n')))
            elif kwargs['json']['format'] is 'xml':
                body = etree.XML(finished['outb'].strip('\n'))
            else:
                #do not add strip here unless you know what you are doing
                body = finished['outb']
        except ValueError:
            body = None

        return resp, body

    def get(self, url, **kwargs):
        return self._request(url, 'GET', **kwargs)

    def post(self, url, **kwargs):
        return self._request(url, 'POST', **kwargs)

    def put(self, url, **kwargs):
        return self._request(url, 'PUT', **kwargs)

    def delete(self, url, **kwargs):
        return self._request(url, 'DELETE', **kwargs)

    def log_wrapper(self):
        """
        Wrapper to set logging parameters for output
        """
        log = logging.getLogger('client.py')

        # Set the log format and log level
        try:
            debug = self.params["debug"]
            log.setLevel(logging.DEBUG)
        except KeyError:
            log.setLevel(logging.INFO)

        # Set the log format.
        stream = logging.StreamHandler()
        logformat = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%b %d %H:%M:%S')
        stream.setFormatter(logformat)

        log.addHandler(stream)
        return log
