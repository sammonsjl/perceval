# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Jamie Sammons <jamie.sammons@liferay.com>
#

import json
import logging

import requests
from grimoirelab_toolkit.uris import urijoin
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from perceval.client import HttpClient

MAX_RESULTS = 100  # Maximum number of results per query

logger = logging.getLogger(__name__)


class LiferayClient(HttpClient):
    """Liferay API client.

    This class implements a simple client to retrieve entities from
    any Liferay system.

    :param url: URL of the Liferay server
    :param group_id: Liferay Site to fetch data from
    :param user: Liferay's username
    :param password: Liferay's password
    :param verify: allows to disable SSL verification
    :param cert: SSL certificate
    :param max_results: max number of results per query
    :param archive: an archive to store/read fetched data
    :param from_archive: it tells whether to write/read the archive

    :raises HTTPError: when an error occurs doing the request
    """
    RESOURCE = 'api/jsonws'
    BLOG = 'blogs.blogsentry'
    MBCATEGORY = 'mb.mbcategory'
    MBCATEGORY_ID = 'categoryId'
    MBMESSAGE = 'mb.mbmessage'
    GROUP = 'group'
    GROUP_ID = 'group-id'
    POST = "POST"
    STATUS = 0

    def __init__(self, url, group_id, user=None, password=None,
                 verify=None, cert=None,
                 max_results=MAX_RESULTS,
                 archive=None, from_archive=False):
        super().__init__(url, archive=archive, from_archive=from_archive)

        self.url = url
        self.group_id = group_id
        self.user = user
        self.password = password
        self.verify = verify
        self.cert = cert
        self.max_results = max_results

        if not from_archive:
            self.__init_session()

    def get_blogs(self, group_id):
        """
        Retrieve all blog entries from Liferay Site

        :param group_id: Liferay Site to fetch data from
        """
        blogs_count = self.__get_blogs_count(group_id)

        url = urijoin(self.base_url, self.RESOURCE, self.BLOG,
                      'get-group-entries')

        blogs = self.get_entries(url, blogs_count, group_id)

        return blogs

    def get_entries(self, url, total, group_id, mbcategory_id):
        """Retrieve all the items from a given Liferay Site.

        :param url: endpoint API url
        :param total: Total number of items to fetch
        :param group_id: Liferay Site to fetch data from
        :param mbcategory_id: Message Board category to fetch messages from
        """
        start = 0
        end = self.max_results

        req = self.fetch(url, method=self.POST,
                         payload=self.__build_payload(start, end, group_id, mbcategory_id))

        entries = req.text

        start += min(self.max_results, total)
        end += min(self.max_results, total)
        self.__log_status(start, total, url)

        while entries:
            yield entries
            entries = None

            if end <= total:
                req = self.fetch(url, method=self.POST,
                                 payload=self.__build_payload(start, end,
                                                              group_id, mbcategory_id))

                start += self.max_results
                end += self.max_results
                entries = req.text
                self.__log_status(start, total, url)

    def get_mbcategory_ids(self, group_id):
        """
        Retrieve all message board categories from Liferay Site

        :param group_id: Liferay Site to fetch data from
        """
        url = urijoin(self.base_url, self.RESOURCE, self.MBCATEGORY,
                      'get-categories', self.GROUP_ID, group_id)
        req = self.fetch(url)

        categories = json.loads(req.text)

        category_ids = []

        for category in categories:
            category_ids.append(category['categoryId'])

        return category_ids

    def get_mbmessages(self, group_id, mbcategory_id):
        """
        Retrieve all message board messages from Liferay Site

        :param group_id: Liferay Site to fetch data from
        :param mbcategory_id: Message Board category to fetch messages from
        """
        mbmessages_count = self.__get_mbcategory_messages_count(group_id, mbcategory_id)

        url = urijoin(self.base_url, self.RESOURCE, self.MBMESSAGE,
                      'get-category-messages')

        mbmessages = self.get_entries(url, mbmessages_count, group_id, mbcategory_id)

        return mbmessages

    def __build_payload(self, start, end, group_id, mbcategory_id=0):
        payload = {'groupId': group_id,
                   'categoryId': mbcategory_id,
                   'status': self.STATUS,
                   'start': start,
                   'end': end}

        return payload

    def __get_blogs_count(self, group_id):
        url = urijoin(self.base_url, self.RESOURCE, self.BLOG,
                      'get-group-entries-count', self.GROUP_ID, group_id,
                      'status',
                      self.STATUS)

        req = self.fetch(url)

        blogs_count = req.text

        return int(blogs_count)

    def __get_mbcategory_messages_count(self, group_id, mbcategory_id):
        url = urijoin(self.base_url, self.RESOURCE, self.MBMESSAGE,
                      'get-category-messages-count', self.GROUP_ID, group_id,
                      self.MBCATEGORY_ID, mbcategory_id, 'status',
                      self.STATUS)

        req = self.fetch(url)

        mbmessage_count = req.text

        return int(mbmessage_count)

    def __init_session(self):
        if (self.user and self.password) is not None:
            self.session.auth = (self.user, self.password)

        if self.cert:
            self.session.cert = self.cert

        if self.verify is not True:
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
            self.session.verify = False

    def __log_status(self, max_items, total, url):
        if total != 0:
            nitems = min(max_items, total)
            logger.info("Fetching %s/%s items from %s" % (nitems, total, url))
        else:
            logger.info("No items were found for %s." % url)
