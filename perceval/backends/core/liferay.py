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
from ...backend import (Backend,
                        BackendCommand,
                        BackendCommandArgumentParser)

CATEGORY_BLOG = "blog"
CATEGORY_MESSAGE = "message"
CATEGORY_USER = "user"
MAX_RESULTS = 100  # Maximum number of results per query

logger = logging.getLogger(__name__)


class Liferay(Backend):
    """Liferay backend for Perceval.

    This class retrieves blog entries and forum messages stored
    in Liferay system.To initialize this class the URL must be provided.
    The `url` will be set as the origin of the data.

    :param url: URL of the Liferay server
    :param group_id: Liferay Site to fetch data from
    :param user: Liferay's username
    :param password: Liferay's password
    :param verify: allows to disable SSL verification
    :param cert: SSL certificate
    :param max_results: max number of results per query
    :param tag: label used to mark the data
    :param archive: archive to store/retrieve items
    """
    version = '0.1.0'

    CATEGORIES = [CATEGORY_BLOG, CATEGORY_MESSAGE, CATEGORY_USER]

    def __init__(self, url, group_id,
                 user=None, password=None,
                 verify=True, cert=None,
                 max_results=MAX_RESULTS, tag=None,
                 archive=None):
        origin = url

        super().__init__(origin, tag=tag, archive=archive)
        self.url = url
        self.group_id = group_id
        self.user = user
        self.password = password
        self.verify = verify
        self.cert = cert
        self.max_results = max_results
        self.client = None

    def fetch(self, category=CATEGORY_USER):
        """Fetch the entries from the site.

        The method retrieves, from a Liferay site

        :param category: the category of items to fetch

        :returns: a generator of issues
        """

        items = super().fetch(category)

        return items

    def fetch_items(self, category, **kwargs):
        """Fetch the items (issues or pull_requests)

        :param category: the category of items to fetch
        :param kwargs: backend arguments

        :returns: a generator of items
        """

        logger.info("Fetching Liferay users from site '%s'", self.url)

        raw_user_pages = self.client.get_users(self.group_id)

        identities = {}

        for raw_user_page in raw_user_pages:
            users = self.parse_entries(raw_user_page)
            for user in users:
                identities.update(self.get_identity(user))
                yield user

        logger.info("Fetching Liferay blog entries from site '%s'", self.url)

        raw_blog_pages = self.client.get_blogs(self.group_id)

        for raw_blog_page in raw_blog_pages:
            entries = self.parse_entries(raw_blog_page)
            for entry in entries:
                if entry['userId'] in identities:
                    entry['screenName'] = identities[entry['userId']]['screenName']
                    entry['emailAddress'] = identities[entry['userId']]['emailAddress']

                yield entry

        logger.info("Fetching Liferay messages entries from site '%s'", self.url)

        mbcategory_ids = self.client.get_mbcategory_ids(self.group_id)

        for mbcategory_id in mbcategory_ids:
            raw_message_pages = self.client.get_mbmessages(self.group_id, mbcategory_id)

            for raw_message_page in raw_message_pages:
                messages = self.parse_entries(raw_message_page)
                for message in messages:
                    if message['userId'] in identities:
                        message['screenName'] = identities[message['userId']]['screenName']
                        message['emailAddress'] = identities[message['userId']]['emailAddress']

                    yield entry

    @classmethod
    def has_archiving(cls):
        """Returns whether it supports archiving items on the fetch process.

        :returns: this backend supports items archive
        """
        return True

    @classmethod
    def has_resuming(cls):
        """Returns whether it supports to resume the fetch process.

        :returns: this backend supports items resuming
        """
        return False

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a Liferay item."""

        return str(item['uuid'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a Liferay item.

        The timestamp is extracted from 'modifiedDate' field.
        This date is a UNIX timestamp but needs to be converted to
        a float value.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        return float(item['modifiedDate'] / 1000)

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a Liferay item.

        This backend generates two types of item which are
        'blog' and 'message'.
        """

        if "entryId" in item:
            category = CATEGORY_BLOG
        elif "messageId" in item:
            category = CATEGORY_MESSAGE
        else:
            category = CATEGORY_USER

        return category

    @staticmethod
    def parse_entries(raw_page):
        """Parse a Liferay API raw response.

        The method parses the API response retrieving the
        entries from the received items

        :param items: items from where to parse the questions

        :returns: a generator of entries
        """
        entries = json.loads(raw_page)
        for entry in entries:
            yield entry

    @staticmethod
    def get_identity(user):

        identity = {user['userId']: {'screenName': user['screenName'], 'emailAddress': user['emailAddress']}}

        return identity

    def _init_client(self, from_archive=False):
        """Init client"""

        return LiferayClient(self.url, self.group_id, self.user, self.password,
                             self.verify, self.cert, self.max_results,
                             self.archive, from_archive)


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
    POST = 'POST'
    STATUS = 0
    USER = 'user'

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
                      'get-group-entries', self.GROUP_ID, group_id)

        blogs = self.get_entries(url, blogs_count)

        return blogs

    def get_entries(self, url, total):
        """Retrieve all the items from a given Liferay Site.

        :param url: endpoint API url
        :param total: Total number of items to fetch
        """
        start = 0
        end = self.max_results

        req = self.fetch(url, method=self.POST,
                         payload=self.__build_payload(start, end))

        entries = req.text

        start += min(self.max_results, total)
        end += min(self.max_results, total)
        self.__log_status(start, total, url)

        while entries:
            yield entries
            entries = None

            if start + self.max_results < total:
                req = self.fetch(url, method=self.POST,
                                 payload=self.__build_payload(start, end))

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

        self.__log_status(len(categories), len(categories), url)

        return category_ids

    def get_mbmessages(self, group_id, mbcategory_id):
        """
        Retrieve all message board messages from Liferay Site

        :param group_id: Liferay Site to fetch data from
        :param mbcategory_id: Message Board category to fetch messages from
        """
        mbmessages_count = self.__get_mbcategory_messages_count(group_id, mbcategory_id)

        url = urijoin(self.base_url, self.RESOURCE, self.MBMESSAGE,
                      'get-category-messages', self.GROUP_ID, group_id,
                      self.MBCATEGORY_ID, mbcategory_id)

        mbmessages = self.get_entries(url, mbmessages_count)

        return mbmessages

    def get_users(self, group_id):
        """
        Retrieve users' screen name and email address from Liferay Site

        :param site_id: Liferay Site to fetch data from
        """
        user_count = self.__get_user_count(group_id)

        url = urijoin(self.base_url, self.RESOURCE, '/user/get-group-users', self.GROUP_ID, group_id)

        users = self.get_entries(url, user_count)

        return users

    def __build_payload(self, start, end):
        payload = {'status': self.STATUS,
                   'start': start,
                   'end': end,
                   '-obc': ""}

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

    def __get_user_count(self, group_id):
        url = urijoin(self.base_url, self.RESOURCE, self.USER,
                      'get-group-users-count', self.GROUP_ID, group_id,
                      'status', self.STATUS)

        req = self.fetch(url)

        user_count = req.text

        return int(user_count)

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
            logger.info("No items were found for %s" % url)


class LiferayCommand(BackendCommand):
    """Class to run Liferay backend from the command line."""

    BACKEND = Liferay

    @classmethod
    def setup_cmd_parser(cls):
        """Returns the Liferay argument parser."""

        parser = BackendCommandArgumentParser(cls.BACKEND,
                                              basic_auth=True,
                                              archive=True)

        # Liferay options
        group = parser.parser.add_argument_group('Liferay arguments')
        group.add_argument('--site-id', dest='group_id',
                           help="Site to fetch data from")
        group.add_argument('--verify', default=True,
                           help="Value 'False' disables SSL verification")
        group.add_argument('--cert',
                           help="SSL certificate path (PEM)")
        group.add_argument('--max-results', dest='max_results',
                           type=int, default=MAX_RESULTS,
                           help="Maximum number of results requested in the same query")

        # Required arguments
        parser.parser.add_argument('url',
                                   help="Liferay's url")

        return parser
