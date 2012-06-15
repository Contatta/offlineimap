# GridFS storage support
#
# Copyright (c) 2012 Stuart Carnie
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from .Base import BaseFolder
from email.parser import Parser
import dateutil.parser
import email
import time
from datetime import datetime

class GridFSFolder(BaseFolder):

    MODIFY_DATE_KEY = 'modifyDate'

    def __init__(self, db, gfs, files, name, repository):
        """

        @param db: mongodb database
        @type db: Database
        @param gfs: gridFS
        @type gfs: GridFS
        @param files: gridfs files collection
        @type files: pymongo.collection.Collection
        @param name: Folder name
        @type name: unicode
        @param repository: Repository
        @type repository: GridFSRepository
        """

        self.sep = '.'

        super(GridFSFolder, self).__init__(name, repository)

        self._db = db
        self._files = files
        self._gfs = gfs

        #: @type: dict
        self.messagelist = None

    def get_uidvalidity(self):
        """Retrieve the current connections UIDVALIDITY value

        GridFS store have no notion of uidvalidity, so we just return a magic
        token."""
        return 27

    def cachemessagelist(self):
        if self.messagelist is None:
            self.messagelist = self._loadMessageList()

    def getmessagelist(self):
        return self.messagelist

    def getmessage(self, uid):
        file = self._gfs.get_last_version(uid=uid)
        retval = file.read()
        file.close()
        return retval

    def getmessagetime(self, uid):
        file = self._gfs.get_last_version(uid=uid)
        modifyDate = file.modifyDate
        return time.mktime(modifyDate.utctimetuple())

    def addressToList(self, address):
        addresslist = email.utils.getaddresses([address])
        return [{ 'displayName': i[0], 'email': i[1]} for i in addresslist]

    def savemessage(self, uid, content, flags, rtime):

        self.ui.savemessage('gridfs', uid, flags, self)

        if uid < 0:
            # as per maildir implementation, we cannot create new uids
            return uid

        if uid in self.messagelist:
            # already have message, just update flags
            self.savemessageflags(uid, flags)
            return uid

        #: @type: email.Message
        msg = Parser().parsestr(content, headersonly=True)
        fromAddress = self.addressToList(msg['From'])[0]

        if (msg.has_key('To') and msg['To'] is not None):
            toAddress = self.addressToList(msg['To'])
        else:
            toAddress = None

        if msg.has_key('CC'):
            ccAddress = self.addressToList(msg['CC'])
        else:
            ccAddress = None

        sent = None
        sent_date = msg['Date'] if msg.has_key('Date') else None
        messageid = msg['Message-Id'] if msg.has_key('Message-Id') else None
        if sent_date is not None:
            try:
                sent = dateutil.parser.parse(sent_date, fuzzy=True)
            except Exception:
                self.ui.warn("Processing message %s [acc: %s]:\n Date not valid, value: %s" %\
                                              (uid, self.accountname, sent_date))

        obj = {
            'uid': uid,
            'type': 'M',
            'flags': [flag for flag in flags],
            GridFSFolder.MODIFY_DATE_KEY: datetime.utcnow(),
            'filename': self.format_file_name(uid),
            'path': self.name,
            'accountname': self.accountname,

            # mail specific metadata
            'mailHeaders' : {
                'subject': msg['Subject'],
                'from': fromAddress,
                'date': sent,
                'to': toAddress,
                'cc': ccAddress,
                'messageid':messageid
            }
        }

        id = self._gfs.put(content, **obj)
        self.messagelist[uid] = { 'flags': flags, '_id' : id }

        self.ui.debug('gridfs', 'savemessage: returning uid %d' % uid)

        return uid

    def getmessageflags(self, uid):
        return self.messagelist[uid]['flags']

    def savemessageflags(self, uid, flags):
        id = self.messagelist[uid]['_id']
        self._files.update({'_id' : id}, { '$set': { 'flags': [flag for flag in flags], GridFSFolder.MODIFY_DATE_KEY: datetime.utcnow() }})
        self.messagelist[uid]['flags'] = flags

    def change_message_uid(self, uid, new_uid):
        msg = self.messagelist[uid]
        id = msg['_id']
        self._files.update({'_id' : id}, { '$set': { 'filename': self.format_file_name(new_uid), 'uid': new_uid, GridFSFolder.MODIFY_DATE_KEY: datetime.utcnow() } })
        self.messagelist[new_uid] = msg
        del(self.messagelist[uid])

    def format_file_name(self, uid):
        """
        Generates a mail-dir style file name

        @param uid: unique message identifier
        @type uid: string
        @return: formatted file name
        @rtype: string
        """
        return '{0}/{1}/{2}'.format(self.accountname, self.name, unicode(uid))

    def deletemessage(self, uid):
        if not self.uidexists(uid):
            return

        self._gfs.delete(self.messagelist[uid]['_id'])
        del(self.messagelist[uid])

    # region private

    def _loadMessageList(self):
        """Cache the message list from a Maildir.

        Maildir flags are: R (replied) S (seen) T (trashed) D (draft) F
        (flagged).
        :returns: dict that can be used as self.messagelist"""
        maxage = self.config.getdefaultint("Account " + self.accountname,
                                           "maxage", None)
        maxsize = self.config.getdefaultint("Account " + self.accountname,
                                            "maxsize", None)

        retval = {}

        files = self._files.find({'accountname':self.accountname, 'path':self.name, 'type':'M'})
        for file in files:
            retval[file['uid']] = { 'flags': set(file['flags']), '_id' : file['_id'] }

        return retval

    # endregion
