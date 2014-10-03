# coding=utf-8
__author__ = 'Matteo'

from NuovoParser import *


class Protein(object):
    def __init__(self, name, classes, race, pfam, supfam, prints, prosite, interpro, eggnog, smart, keywords, dataset):
        """
        Oggetto proteina contenente tutte le sue caratteristiche
        :param dataset: bp = Biological Process, cc = Cellular Component, mf = Molecular Function
        :param classes: classi Gene Ontology delle proteine
        :param race: hs = Homo Sapiens, at = Arabidopsis Thaliana
        :param pfam: protein domain
        :param supfam: structural and functional annotations
        :param prints: motif fingerprints
        :param prosite: protein domains and families
        :param interpro: integrated resource of protein families, domains and functional sites
        :param eggnog: evolutionary genealogy of genes: Non-supervised Orthologous Groups
        :param smart: Simple Modular Architecture Research Tool (database annotations)
        :param keywords: Manually curated keywords describing the function of the proteins
        """
        racemap = {'at': 'Arabidopsis Thaliana', 'hs': 'Homo Sapiens'}
        datasetmap = {'bp': 'Biological Process', 'cc': 'Cellular Component', 'mf': 'Molecular Function'}
        self.dataset = datasetmap[dataset.lower()]
        self.name = name
        self.classes = classes
        self.race = racemap[race.lower()]
        self.keywords = keywords
        self.pfam = pfam
        self.supfam = supfam
        self.prints = prints
        self.prosite = prosite
        self.interpro = interpro
        self.eggnog = eggnog
        self.smart = smart


class ElabData(object):
    def __init__(self, dataset, race, partition='leaves', inputdb='gotree3', outputdb='dbgo'):
        """
        Legge le classi contenute nel database inputdb, determina le proteine che hanno quella classe
        dal database outputdb e restituisce un dizionario contenente come chiavi le classi
        e come valore gli oggetti proteina.
        :param partition: 'all': all nodes, 'leaves': only leaf nodes, 'root': only root node
        :param race: hs = Homo Sapiens, at = Arabidopsis Thaliana
        :param dataset: bp = Biological Process, cc = Cellular Component, mf = Molecular Function
        :param inputdb: nome del database contenente i grafi
        :param outputdb: nome del database contenente le annotazioni ed i profili binari
        """
        datasets = ['bp', 'cc', 'mf']
        self.race = race
        self.dataset = dataset
        self.others = filter(lambda x: x != self.dataset, datasets)
        self.outputdb = outputdb
        self.inputdb = inputdb
        self.intab = []
        self.outtab = []
        self.dbin = DataBase(inputdb)
        self.dbout = DataBase(outputdb)
        self.match = {self.inputdb: self.dbin, self.outputdb: self.dbout}
        self.nodes = []
        self.proteins = []
        self.partition = partition
        self.proteinlist = []

    @staticmethod
    def cleanlist(sequence):
        """
        Converte una lista contenente liste con un solo elemento in una lista coi soli elementi
        :param sequence: lista contenente liste con un solo elemento
        :return: lista coi soli elementi
        """
        if not sequence:
            return sequence
        return [elem[0] for elem in sequence]

    def gettabnames(self, dbname):
        """
        cerca i nomi di tutte le tabelle (non di sistema) presenti nel database dbname
        :param dbname: nome del database
        """
        db = self.match[dbname]
        data = db.select('information_schema.tables', 'table_name',
                         "table_schema =  'public' AND table_type='BASE TABLE';")
        if dbname == self.inputdb:
            self.intab = self.cleanlist(data)
            self.intab = [elem for elem in self.intab if self.dataset in elem and self.race in elem]
        else:
            self.outtab = self.cleanlist(data)
            self.outtab = [elem for elem in self.outtab if self.race in elem and elem[-2:] not in self.others]

    def getnodes(self, tabname, partition='all'):
        """
        Seleziona i nodi dalla tabella tabname che corrispondono al criterio scelto tramite il parametro partition
        :param tabname: nome della tabella
        :param partition: nodi del grafo delle classi da selezionare. 'all' = tutti, 'leaves' = foglie, 'root' = radice
        :return: lista dei nodi trovata
        """
        command = {'all': None, 'leaves': 'nchild = 0', 'root': 'npar = 0'}
        self.nodes = self.cleanlist(self.dbin.select(tabname, 'class', command[partition]))
        return self.nodes

    def getproteins(self, tabname, node):
        """
        Dato un nodo node e una tabella, estrae le proteine che hanno quel nodo come attributo
        :param tabname: nome della tabella i nomi delle proteine  gli attributi di ciascuna proteina
        :param node: nome del nodo a cui associare la proteina
        :return: lista dei nomi delle proteine trovate
        """
        self.proteins = self.cleanlist(self.dbout.select(tabname, 'nome', "'%s' = ANY(attributi)" % node))
        return self.proteins

    def setprotein(self, protein):
        """
        Crea l' oggetto proteina 'protein' con gli attributi ricavati dalle tabelle elencate in self.outtab
        :param protein: nome della proteina
        :return: oggetto Protein
        """
        result = []
        for table in self.outtab:
            field = (lab.cleanlist(lab.dbout.select(table, 'attributi', "nome = '%s'" % protein)))
            if field:
                result.append(field[0])
            else:
                result.append(field)
        return Protein(protein, result[0], self.race, result[4], result[8], result[5], result[6], result[3], result[2],
                       result[7], result[1], self.dataset)

    def elabproteins(self):
        """
        Restituisce la lista degli oggetti Protein basato sulle proteine elencate in self.proteins
        :return: lista di oggetti Protein
        """
        proteinlist = []
        if self.proteins:
            for protein in self.proteins:
                proteinlist.append(self.setprotein(protein))
        return proteinlist

    def elab(self):
        """
        Per ciascun nodo estratto dal database self.inputdb associa la lista di proteine da self.outputdb.
        restituisce un dizionario che associa al nodo un oggetto Protein
        :return: dizionario{nodo: oggetto Protein}
        """
        result = dict()
        self.gettabnames(self.inputdb)
        self.gettabnames(self.outputdb)
        nodes = self.getnodes(self.intab[0], self.partition)
        for node in nodes:
            print 'elaborazione nodo', node
            p = self.getproteins(self.outtab[0], node)
            if p:
                result[node] = self.elabproteins()
        self.proteinlist = result
        return result


if __name__ == '__main__':
    lab = ElabData('bp', 'at')
    for key, item in lab.elab().items():
        print key, item
