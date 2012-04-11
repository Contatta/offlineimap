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

from offlineimap.ui import getglobalui
from offlineimap.error import OfflineImapError
from offlineimap.repository.Base import BaseRepository
from offlineimap.folder.GridFS import GridFSFolder
from pymongo import Connection
from gridfs import *

class GridFSRepository(BaseRepository):
    def __init__(self, reposname, account):
        BaseRepository.__init__(self, reposname, account)

        self._dbName = None
        self._uri = None

        self.folders = None
        self.ui = getglobalui()
        self.debug("GridFSRepository initialized, sep is " + repr(self.getsep()))
        self.host = self.config
        self._cn = Connection(self.geturi())
        info = self._cn.server_info()

        self._db = self._cn[self.getdb()]
        if (not self._db[self.getCollectionName()]):
            self._db.create_collection(self.getCollectionName())

        #: :type: Collection
        self._files = self._db[self.getCollectionName()].files
        self._files.ensure_index('accountname')
        self._files.ensure_index('uid')

        self._gfs = GridFS(self._db, self.getCollectionName())

    def connect(self):
        pass

    def getsep(self):
        return self.getconf('sep', '.').strip()

    def getNamespacePrefix(self):
        return self.getconf('prefix', 'mail').strip()

    def geturi(self):
        if self._uri:
            return self._uri

        uri = self.getconf('uri', None)
        if uri is not None:
            self._uri = uri
            return self._uri

        # no success
        raise OfflineImapError("No uri specified for repository "\
                                   "'%s' specified." % self,
                               OfflineImapError.ERROR.REPO)


    def getdb(self):
        if self._dbName:
            return self._dbName

        dbName = self.getconf('db', None)
        if dbName != None:
            self._dbName = dbName
            return self._dbName

        # no success
        raise OfflineImapError("No db for repository "\
                                   "'%s' specified." % self,
                               OfflineImapError.ERROR.REPO)

    def getCollectionName(self):
        return self.getNamespacePrefix()

    def deletefolder(self, foldername):
        self.ui.warn("NOT YET IMPLEMENTED: DELETE FOLDER %s" % foldername)

    def getfolder(self, foldername):
        """Return a Folder instance of this GridFS

        @type foldername: unicode
        @param foldername: Name of the folder

        @rtype: GridFSFolder
        @return unicode: the folder value
        """

        folders = self.getfolders()
        for folder in folders:
            if foldername == folder.name:
                return folder

        raise OfflineImapError("getfolder() asked for a non-existing "
                               "folder '%s'." % foldername,
                               OfflineImapError.ERROR.FOLDER)

    def getfolders(self):
        """
        Returns a list of all folders for this server

        @return: List of folders
        @rtype: list
        """

        if self.folders is None:
            self.folders = self._scan_folders()

        return self.folders

    def forgetfolders(self):
        self.folders = None

    def makefolder(self, foldername):
        self.ui.makefolder(self, foldername)

        self._gfs.put('',
            accountname = self.accountname,
            path = foldername,
            filename = foldername,
            type = 'D',
            securitygroup = -1
        )

    def debug(self, msg):
        self.ui.debug('gridfs', msg)

    # region private

    def _scan_folders(self):
        """
        Returns a list of all folders for this server

        @return: List of folders
        @rtype: list
        """

        retval = []
        files = self._files.find({ 'type': 'D' })
        for file in files:
            retval.append(GridFSFolder(self._db, self._gfs, self._files, file['path'], self))

        return retval


    # endregion