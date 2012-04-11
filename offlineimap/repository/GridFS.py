# GridGS repository support

from offlineimap import folder
from offlineimap.ui import getglobalui
from offlineimap.error import OfflineImapError
from offlineimap.repository.Base import BaseRepository
from offlineimap.folder.GridFS import GridFSFolder
from pymongo import Connection
from gridfs import *

class GridFSRepository(BaseRepository):
    def __init__(self, reposname, account):
        BaseRepository.__init__(self, reposname, account)

        self._host = None
        self._dbName = None

        self.folders = None
        self.ui = getglobalui()
        self.debug("GridFSRepository initialized, sep is " + repr(self.getsep()))
        self.host = self.config
        self._cn = Connection(self.gethost())
        self._db = self._cn[self.getdb()]
        if (not self._db[self.getCollectionName()]):
            self._db.create_collection(self.getCollectionName())

        self._files = self._db[self.getCollectionName()].files
        self._gfs = GridFS(self._db, self.getCollectionName())

    def connect(self):
        pass

    def getsep(self):
        return self.getconf('sep', '.').strip()

    def gethost(self):
        if self._host:
            return self._host

        host = self.getconf('remotehost', None)
        if host != None:
            self._host = host
            return self._host

        # no success
        raise OfflineImapError("No remote host for repository "\
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
        return self.accountname

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