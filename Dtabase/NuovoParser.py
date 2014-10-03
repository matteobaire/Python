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
        :type path: stringa
        :param self.dirs: lista contenente i nomi delle stringhe
        :type self.dirs: lista di stringhe
        :param self.len: numero di directory presente nel percorso iniziale
        :type self.len: intero
        :param self.files: dizionario che associa ad ogni directory i file contenuti
        :type self.files: dizionario{stringa : lista di stringhe}
        """
        self.path = path
        self.dirs = [dname for dname in os.listdir(self.path) if os.path.isdir(path + dname)]
        self.len = len(self.dirs)
        self.files = {self.dirs[index]: os.listdir(self.path + '\\' + self.dirs[index]) for index in xrange(self.len)}

    def nfiles(self, dirname=None):
        """
        Dato il nome di una directory restituisce i nomi dei file contenuti in essa
        :param dirname: nome della directory
        :type dirname: stringa
        :return: lista dei nomi dei file contenuti nella directory
        :rtype: lista di stringhe
        """
        if dirname not in self.dirs:
            print 'directory non trovata'
            return None
        return self.files[dirname]

    def extfiles(self, dirname=None, ext=''):
        """
        Dati il nome di una directory e un estensione, restituisce tutti i nomi dei file con quell' estensione
        :param dirname: nome della directory
        :type dirname: stringa
        :param ext: estensione del file
        :type ext: stringa
        :return: lista coi nomi dei file aventi estensione 'ext'
        :rtype:  lista di stringhe
        """
        ext = '.' + ext
        allfiles = self.nfiles(dirname)
        zipfiles = [allfiles[findex] for findex in xrange(len(allfiles)) if ext in allfiles[findex][-4:]]
        return zipfiles

    def getpath(self, namefile):
        """
        Dato il nome di un file restituisce il percorso completo del file
        :param namefile: nome del file completo
        :type namefile: stringa
        :return: stringa contenente il percorso completo del file
        :rtype: stringa
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
        :type filename: stringa
        :param path: percorso completo del file
        :type path: stringa
        :param self.exdata: dizionario che contiene il prodotto del metodo datalab
        :type self.exdata: dizionario{stringa: lista di stringhe} o dizionario{stringa: lista di liste di stringhe}
        :param self.tabname: contiene il nome del file senza punti e senza estensione
        :type self.tabname: stringa
        :param self.ext: contiene l'estensione del file
        :type self.ext: stringa
        :param self.filename: contiene il nome del file con estensione txt
        :type self.filename: stringa
        :param self.filezip: contiene il nome del file con la sua estensione
        :type self.filezip: stringa
        :param self.text: contiene il contenuto del file
        :type self.text: stringa
        :param self.data: contiene i dati estratti da self.text
        :type self.data: lista di liste di stringhe
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
        Carica il file, estrae i dati dal file e salva tutto nell' attributo self.data
        Il comportamento è diverso a seconda dell' estensione del file:
        * per i file zip viene letto il file txt contenuto in esso
        * per i file non zip viene letto direttamente il file
        viene rilevato automaticamente il tipo di separatore (riconosce solo la virgola o lo spazio)
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
        :type lista: lista di stringhe
        :return: lista ripulita
        :rtype: lista di stringhe
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
        e memorizza il risultato in self.tabname
        """
        self.tabname = self.filezip.replace('.', '').replace(self.ext, '')

    def datalab(self):
        """
        Elabora i dati in modo che self.tabname, self.names e self.exdata contengano rispettivamente:
        * il nome della tabella postgres in cui verranno salvati i dati
        * gli elementi della prima colonna della tabella
        * un dizionario {elemento prima colonna: elemento seconda colonna}
        gli elementi della prima colonna sono stringhe, mentre quelli della seconda sono una lista di stringhe
        self.exdata può venire modificato tramite self.mapper() (vedi metodo)
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
        """
        Se l' estensione del file è csv sostituisce self.exdata con un dizionario contenente le stesse chiavi
        ma come valore una lista contenente due liste di stringhe.
        :return: self.exdata originale o il nuovo dizionario creato per i file .csv
        """
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
        :param dbname: nome database
        :param self.tabname: nome tabella
        :param self.con: connessione con postgres
        :type self.con: oggetto connessione
        :param self.cur: cursore
        :type self.cur: oggetto cursore
        :param self.rows: lista di elementi da inserire nel database
        """
        self.dbname = dbname
        self.creadb()
        self.tabname = ''
        self.con = None
        self.cur = None
        self.rows = []

    def openconnection(self):
        """
        Apre un canale con postgres e crea un cursore. Da usare solo se non è richiesta/possibile la connessione con
        un database (i.e.: il database non esiste)
        """
        self.con = psycopg2.connect(user='postgres', host='localhost', password='postgres', port='5433')
        self.con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cur = self.con.cursor()

    def dbload(self, dbname):
        # print 'database ', dbname
        """
        Apre un canale con un database (se esiste) dbname e lo imposta come database corrente.
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
        Crea un database con nome self.dbname. È necessario usare self.openconnection() in quanto
        il database da creare non esiste. Se invece dovesse esistere, viene segnalata la sua esistenza.
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
        Crea la tabella tabname con le colonne 'nome' e 'attributi' (solo se la tabella non è già esistente).
        se il nome del file contiene 'graph' viene creata una tabella con cinque colonne, altrimenti viene creata una
        tabella a con due colonne.
        :param tabname: nome della tabella
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
        aggiunge la colonna 'column' (se non esiste) di tipo 'tipo' alla tabella 'tabname'
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
        * se la tabella in cui inserire i dati è di due colonne inserisce solo gli elementi di data1 non nulli
        * se la tabella è di cinque colonne inserisce tutti gli elementi
        :param tabname: nome della tabella
        :param nome: stringa da inserire nella colonna 'nome' o 'class'  della tabella 'tabname'
        :param data1: lista di stringhe da inserire nella colonna 'attributi' o 'children' della tabella 'tabname'
        :param data2: lista di stringhe da inserire nella colonna 'parent' della tabella 'tabname'
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
        """
        Effettua il select all' interno del database Postgres (self.dbname) i parametri di input cercano di conservare
        la sintassi di SQL.
        :param fromwhere: i parametri da passare a FROM es: pg_database, information_schema.tables
        :param what: i parametri da passare a SELECT (* indica tutto) es: datname, table_name
        :param where: i parametri da passare a WHERE es: table_schema = X, table_type = X
        :return: lista dei risultati della query. Il tipo dei contenuti della lista varia a seconda della query.
        """
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
        :param dbname: nome del database
        :param ext: estensione del file
        :param self.files: lista contenente i file da elaborare
        :param self.link: percorso completo del file
        :param self.path: oggetto PathManager per gestire i percorsi dei file
        :param self.datab: oggetto DataBase per l' inserimento dei dati in Postgres
        """
        self.files = []
        self.link = ''
        self.ext = ext
        self.path = PathManager()
        self.datab = DataBase(dbname)

    def getfiles(self, directory):
        """
        assegna all' attributo self.files la lista di file contenuti nella directory 'directory'
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
        data una directory esegue il metodo self.elabfile per tutti i file della directory
        :param directory: nome directory
        """
        self.getfiles(directory)
        for f in self.files:
            self.elabfile(f)

    def parsing(self):
        """
        esegue il metodo self.elaballfiles per tutte le directory presenti nel percorso iniziale di default
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
