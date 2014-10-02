# coding=utf-8
__author__ = 'Matteo'

import zipfile
import os
import psycopg2
from collections import defaultdict


class PathManager(object):
    def __init__(self, path='C:\\Users\Matteo\\Documents\\Gene Ontology\\'):
        """
        dato un percorso iniziale (percorso di default), trova le directory ed i file contenuti e li salva
        in un dizionario {directory : lista di file contenuti} chiamato self.files
        :param path: nome del percorso iniziale
        :parameter dirs: lista dei nomi delle directory contenute nel percorso iniziale
        """
        self.path = path
        self.dirs = [dname for dname in os.listdir(self.path) if os.path.isdir(path + dname)]
        self.len = len(self.dirs)
        self.files = {self.dirs[index]: os.listdir(self.path + '\\' + self.dirs[index]) for index in xrange(self.len)}

    def nfiles(self, dirname=None):
        """
        :param dirname: nome della directory
        :return: lista dei nomi dei file contenuti nella directory
        """
        if dirname not in self.dirs:
            print 'directory non trovata'
            return None
        return self.files[dirname]

    def extfiles(self, dirname=None, ext=''):
        """
        :param dirname: nome della directory
        :param ext: estensione del file
        :return: lista coi nomi dei file aventi estensione 'ext'
        """
        ext = '.' + ext
        allfiles = self.nfiles(dirname)
        zipfiles = [allfiles[findex] for findex in xrange(len(allfiles)) if ext in allfiles[findex][-4:]]
        return zipfiles

    def getpath(self, namefile):
        """
        :param namefile: nome del file completo
        :return: stringa contenente il percorso completo del file
        """
        for directory in self.files:
            if namefile in self.files[directory]:
                return self.path + directory
        print 'file non trovato'
        return None


class FileManager(object):
    def __init__(self, filename=None, path=None):
        """
        Gestore di file. Gestisce un solo file.
        :param filename: nome del file
        :param path: percorso completo del file
        """
        self.exdata = dict()
        self.tabname = ''
        self.names = []
        self.path = path
        self.ext = filename[-3:]
        self.filename = os.path.splitext(filename)[0] + '.txt'
        self.filezip = filename
        self.text = []
        self.data = []

    def load(self):
        """
        carica il file, estrae i dati dal file e salva tutto nell' attributo self.data
        """
        index = self.path + "\\" + self.filezip
        if 'zip' in self.ext:
            archive = zipfile.ZipFile(index, 'r')
            self.text = archive.read(self.filename)
        else:
            archive = open(index, 'r')
            self.text = archive.read()
        print 'file %s caricato' % self.filezip
        if ',' in self.text:
            separator = ','
        else:
            separator = ' '
        self.data = [fline.split(separator) for fline in self.text.splitlines()]
        self.data = [self.cleanlist(self.data[line]) for line in xrange(len(self.data))]
        archive.close()

    @staticmethod
    def cleanlist(lista):
        """
        elimina eventuali elementi '', '"' o ' ' indesiderati presenti in una lista
        :param lista: lista da ripulire
        :return: lista ripulita
        """
        if '"' in lista[0]:
            lista = [elem.replace('"', '') for elem in lista]
        if '' in lista:
            lista.remove('')
        if ' ' in lista:
            lista.remove(' ')
        return lista

    def gettabname(self):
        """
        Rimuove dal nome di un file i punti e l' estensione in modo che possa essere usato come nome tabella
        """
        self.tabname = self.filezip.replace('.', '').replace(self.ext, '')

    def datalab(self):
        """
        Elabora i dati in modo che self.tabname, self.names e self.exdata contengano rispettivamente:
        il nome della tabella postgres in cui verranno salvati i dati
        gli elementi della prima colonna della tabella
        un dizionario {elemento prima colonna: elemento seconda colonna}
        gli elementi della prima colonna sono stringhe, mentre quelli della seconda sono una lista di stringhe
        """
        self.gettabname()
        attributes = self.data[0]
        dim = len(self.data[1])
        self.names = [self.data[nindex][0] for nindex in xrange(1, dim)]
        values = [self.data[dindex][1:] for dindex in xrange(1, len(self.data))]
        self.exdata = {self.names[n]: [attributes[j] for j in xrange(len(attributes)) if values[n][j] != '0'] for n
                       in xrange(len(self.names))}
        self.exdata = self.mapper()

    def mapper(self):
        if 'csv' not in self.ext:
            return self.exdata
        fathers = defaultdict(list)
        for parent, children in self.exdata.items():
            for child in children:
                fathers[child].append(parent)
        return {node: [self.exdata[node], fathers[node]] for node in self.exdata.keys()}


class DataBase(object):
    def __init__(self, dbname):
        """
        Classe che gestisce un database postgres.
        dbname = nome database
        tabname = nome tabella
        self.con = connessione con postgres
        self.cur = cursore
        self.rows = lista di elementi da inserire nel database
        """
        self.dbname = dbname
        self.creadb()
        self.tabname = ''
        self.con = None
        self.cur = None
        self.rows = []

    def openconnection(self):
        """
        apre un canale con postgres e crea un cursore
        """
        self.con = psycopg2.connect(user='postgres', host='localhost', password='postgres', port='5433')
        self.con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cur = self.con.cursor()

    def dbload(self, dbname):
        # print 'database ', dbname
        """
        apre un canale con un database
        :param dbname: nome del database
        """
        if self.dbexist(dbname):
            try:
                self.con = psycopg2.connect(dbname=dbname, user='postgres', host='localhost', password='postgres',
                                            port='5433')
                self.con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                self.cur = self.con.cursor()
            except Exception as e:
                print ("I am unable to connect to the database: %s" % e)
        else:
            print('il database non esiste')

    def creadb(self):
        """
        crea un database dbname
        """
        self.openconnection()
        if not self.dbexist(self.dbname):
            try:
                self.cur.execute("CREATE DATABASE " + self.dbname)
                print('database %s created' % self.dbname)
            except Exception as e:
                print("errore creadb: %s" % e)
        else:
            print 'database already exists'
        self.cur.close()
        self.con.close()

    def creatab(self, tabname):
        """
        Crea la tabella tabname con le colonne 'nome' e 'attributi'
        :param tabname: nome tabella
        """
        if not self.tabexists(tabname):
            try:
                self.dbload(self.dbname)
                self.tabname = tabname.lower()
                self.cur.execute("CREATE TABLE " + self.tabname + "();")
                if 'graph' in self.tabname:
                    self.addcolumn(tabname, 'class', 'text')
                    self.addcolumn(tabname, 'children', 'text[]')
                    self.addcolumn(tabname, 'nchild', 'integer')
                    self.addcolumn(tabname, 'parent', 'text[]')
                    self.addcolumn(tabname, 'npar', 'integer')
                else:
                    self.addcolumn(tabname, 'nome', 'text')
                    self.addcolumn(tabname, 'attributi', 'text[]')
                print('table %s created' % tabname)
            except Exception as e:
                print('error creatab: %s' % e)
            self.cur.close()
            self.con.close()
        else:
            print ('table already exists')

    def addcolumn(self, tabname, column, tipo):
        """
        aggiunge la colonna 'column' di tipo 'tipo' alla tabella 'tabname'
        :param tabname: nome tabella
        :param column: nome colonna
        :param tipo: tipo dati colonna
        """
        if not self.colexists(tabname, column):
            try:
                self.dbload(self.dbname)
                # self.cur = self.con.cursor()
                self.cur.execute("ALTER TABLE " + tabname + " ADD COLUMN " + column + " " + tipo + ";")
                self.con.commit()
                print ('added %s column' % column)
            except Exception as e:
                print('error addcolumn: %s' % e)
        else:
            print('column already exists')
        self.con.close()
        self.cur.close()

    def insert(self, tabname, nome, data1, data2=None):
        """
        inserisce i dati 'nome' e 'data' nella tabella 'tabname' creata con creatab()
        :param tabname: nome della tabella
        :param nome: stringa da inserire nella colonna nome  della tabella 'tabname'
        :param data1: lista di stringhe da inserire nella colonna attributi della tabella 'tabname'
        """
        data1 = list(data1)
        if data2 is not None:
            nchild = len(data1)
            npar = len(data2)
            if not data1:
                data1 = ['none']
                nchild = 0
            data2 = list(data2)
            if not data2:
                data2 = ['none']
                npar = 0

            sql = "INSERT INTO " + tabname + " VALUES ('" + nome + "', ARRAY%s, %s, ARRAY%s, %s);" % (
                str(data1), nchild, str(data2), npar)
        else:
            sql = "INSERT INTO " + tabname + " VALUES ('" + nome + "', ARRAY%s);" % str(data1)
        if data1 or data2:
            self.dbload(self.dbname)
            self.cur.execute(sql)
            print 'inserted %s' % nome
            self.con.commit()
            self.con.close()
            self.cur.close()

    def select(self, fromwhere, what='*', where=None):
        self.dbload(self.dbname)
        if where:
            sql = "SELECT %s FROM %s WHERE %s" % (what, fromwhere, where)
            #print sql
        else:
            sql = "SELECT %s FROM %s" % (what, fromwhere)
        self.cur.execute(sql)
        self.rows = self.cur.fetchall()
        return self.rows

    def dbexist(self, dbname):
        """
        verifica se il database 'dbname' esiste
        :param dbname: nome del database
        :return: True se esiste, False se non esiste
        """
        self.openconnection()
        self.cur.execute("SELECT datname from pg_database " + self.dbname.lower() + ";")
        self.rows = self.cur.fetchall()
        self.rows = [row[0] for row in self.rows]
        if dbname not in self.rows:
            return False
        return True

    def tabexists(self, tabname):
        """
        verifica se la tabella 'tabname' esiste
        :param tabname: nome della tabella
        :return: True se 'tabname' esiste, False se non esiste
        """
        tabname = tabname.lower()
        self.dbload(self.dbname)
        self.cur.execute(
            "SELECT table_name  FROM information_schema.tables "
            "WHERE table_schema='" + self.dbname + "' AND table_type='BASE TABLE';")
        self.rows = self.cur.fetchall()
        self.rows = [r[0] for r in self.rows]
        if tabname not in self.rows:
            return False
        return True

    def colexists(self, tabname, colname):
        """
        verifica se la colonna 'colname' della tabella 'tabname' esiste
        :param tabname: nome tabella
        :param colname: nome colonna
        :return: True se la colonna esiste, False se non esiste
        """
        self.dbload(self.dbname)
        # print str(tabname)
        self.cur.execute("SELECT column_name FROM information_schema.columns "
                         "WHERE table_schema='" + self.dbname + "' AND table_name = '" + tabname.lower() + "';")
        self.rows = self.cur.fetchall()
        # print self.rows
        self.rows = [ro[0] for ro in self.rows]
        # print self.rows
        if colname not in self.rows:
            return False
        return True


class Parser(object):
    def __init__(self, dbname, ext):
        """
        per ogni file.zip di ogni directory contenuti nel percorso di default file_path crea una tabella e vi inserisce
        i dati estratti dai file.
        """
        self.files = []
        self.link = ''
        self.ext = ext
        self.path = PathManager()
        self.datab = DataBase(dbname)

    def getfiles(self, directory):
        """
        assegna all' attributo self.files la lista di file con estensione zip contenuti nella directory 'directory'
        :param directory: nome della directory
        """
        self.files = self.path.extfiles(directory, self.ext)

    def elabfile(self, namefile):
        """
        dato il file 'namefile' trova il suo percorso completo, ne estrae i dati e li memorizza in una tabella
        :param namefile: nome del file
        """
        self.link = self.path.getpath(namefile)
        datafile = FileManager(namefile, self.link)
        datafile.load()
        datafile.datalab()
        self.datab.creatab(datafile.tabname)
        for key in datafile.exdata.keys():
            dati = datafile.exdata[key]
            if not dati:
                continue
            if type(dati[0]) is list:
                dati1 = dati[0]
                dati2 = dati[1]
            else:
                dati1 = dati
                dati2 = None
            self.datab.insert(datafile.tabname, key, dati1, dati2)

    def elaballfiles(self, directory):
        """
        data una directory esegue il metodo elabfile per tutti i file della directory
        :param directory: nome directory
        """
        self.getfiles(directory)
        for f in self.files:
            self.elabfile(f)

    def parsing(self):
        """
        esegue il metodo elaballfiles per tutte le directori presenti nel percorso iniziale di default
        """
        for directory in self.path.dirs:
            self.elaballfiles(directory)


if __name__ == '__main__':
    # a = Parser('gotree3', 'csv')
    # a.elaballfiles('GOgraphs')
    # a = Parser('prova', 'csv')
    # a.elabfile('at.graph.BP.csv')
    b = DataBase('gotree3')
    lis = b.select('atgraphbp', 'class', 'nchild = 0')
    lis = tuple([i[0] for i in lis])
    print lis[2]
    c = DataBase('dbgo')
    print c.select('atannbp', 'nome, attributi', "'%s' = ANY(attributi)" % lis[2])
