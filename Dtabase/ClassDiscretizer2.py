# coding=utf-8
__author__ = 'Matteo'

from NuovoParser import *
import math
import time


def extract(myfile):
    """
        :param myfile: nome del file di cui si vogliono estrarre i dati
        :return: dati contenuti nel file
        """
    path = PathManager()
    filepath = path.getpath(myfile)
    findata = FileManager(myfile, filepath)
    findata.load()
    return findata.data


class Subset(object):
    def __init__(self, tree, table, features):
        self.tree = tree
        self.table = table
        self.label = self.tree.keys()
        self.classes = self.tree.values()
        self.features = features
        self.value = self.table.values()
        self.ent = self.entropy()
        self.selected = None
        self.selected_tab = self.table

    def __len__(self):
        """
        :return: la lunghezza dell'oggetto Subset è considerata pari al numero di esempi
        """
        return len(self.tree)

    def elemclass(self, myclass):
        """
        Restituisce la lista contenente il nome degli esempi che appartengono alla classe myclass
        :param myclass: classe di cui si vuole trovare quali elementi vi appartengono
        """
        elemlist = []
        for key, value in self.tree.items():
            if myclass in value:
                elemlist.append(key)
        return elemlist

    def getclasses(self):
        """
        :return: lista di tutte le classi associate agli esempi del set considerato
        """
        result = []
        if len(self.tree.values()) == 0:
            return result
        if isinstance(self.tree.values()[0], list):
            for values in self.tree.values():
                for value in values:
                    if value not in result:
                        result.append(value)
            return sorted(result)
        else:
            return sorted(list(set(self.tree.values())))

    def entropy(self):
        """
        Calcola l'entropia del subset
        :return: valore dell'entropia
        """
        ent = 0.0
        myclasses = self.getclasses()
        for cls in myclasses:
            numelem = len(self.elemclass(cls))
            pcs = float(numelem) / self.__len__()
            ent -= pcs * math.log(pcs, 2)
        return ent

    def select_feat(self, chosen_feat_index):
        """
        seleziona una delle feature e memorizza una un dizionario contenente il solo valore della feature desiderata
        :param chosen_feat_index: indice della feature desiderata
        """
        self.selected = chosen_feat_index
        self.selected_tab = {key: value[self.selected] for key, value in self.table.items()}

    def get_feat(self):
        return self.features[self.selected]

    def __getitem__(self, item):
        self.select_feat(item)
        return self.selected_tab


class Set(object):
    def __init__(self):
        self.set = []

    def add_set(self, tree, table, features):
        self.set.append(Subset(tree, table, features))

    def __getitem__(self, item):
        return self.set[item]


class Examples(object):
    """
    Gestisce una serie di esempi i cui dati sono memorizzati in due matrici: una contenente i valori degli attributi
    e l'altro contenente le classi con cui vengono etichettati gli esempi.
    """

    def __init__(self, mydata, mytree):
        self.dim = len(mydata)
        self.names = [mydata[nindex][0] for nindex in xrange(1, self.dim)]
        self.features = {key: value for key, value in enumerate(mydata[0])}
        self.classes = mytree[0]
        self.value = [mydata[dindex][1:] for dindex in xrange(1, self.dim)]
        self.belong = [mytree[dindex][1:] for dindex in xrange(1, self.dim)]

    def maketree(self):
        """
        Crea un dizionario che associa al nome di ciascun esempio le relative classi
        In base al parametro self.filtered il dizionario avrà solo le foglie (se True) o tutte le classi (se False)
        """
        tree = defaultdict(list)
        for n in xrange(len(self.names)):
            for j in xrange(len(self.classes)):
                if self.belong[n][j] != '0' and self.classes[j] != '00':
                    tree[self.names[n]].append(self.classes[j])
        return dict(tree)

    def maketable(self, attribute=None):
        """
        Crea un dizionario che associa al nome dell' esempio il valore della feature selezionata.
        Nel caso il parametro attribute sia Null, vengono associati all'esempio la lista dei valori
        di tutte le features.
        :param attribute: indice della feature attributo da selezionare (None per averli tutti)
        restituisce il dizionario creato
        """
        table = dict()
        for name, value in zip(self.names, self.value):
            if attribute is None:
                table[name] = value
            else:
                table[name] = value[attribute]
        return table


if __name__ == '__main__':
    name1 = 'Gasch.Spellman.exprs.Tree.csv'
    name2 = 'Gasch.Spellman.exprs.Data.csv'
    a = extract(name1)
    b = extract(name2)
    c = Examples(b, a)
    myitem = Set()
    myitem.add_set(c.maketree(), c.maketable(), c.features)
    print 'esempi:', myitem[0].label
    print 'classi:', myitem[0].classes
    print 'dimensione:', len(myitem[0])
    print 'entropia:', myitem.set[0].entropy()
    print len(myitem[0].features)
    print myitem[0][-1]
    print myitem[0][len(myitem.set[0].features) - 1]




