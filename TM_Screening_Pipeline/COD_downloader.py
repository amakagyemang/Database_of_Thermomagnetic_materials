import os
import tqdm
import numpy as np
import pandas as pd
import shutil
from pymatgen.io.cif import CifParser
import pymatgen.symmetry.analyzer
from pymatgen.io.vasp import Poscar

# _cod_database_code
# _chemical_formula_sum
# _chemical_formula_structural
# _symmetry_cell_setting lattice system
# _space_group_IT_number spacegroup
# elements -split from  _chemical_formula_structural
# _cell_volume
# _journal_paper_doi
# _publ_section_title

# unique sites -calculate from structure and elements


def make_path_list():
    directory = 'D:/cif/'
    listing = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    with open('all_paths', "w+") as f:
        for i in listing:
            listing2 = (os.listdir(directory+str(i)))
            for k in listing2:
                listing3 = (os.listdir(directory+str(i) + '/' + str(k)))
                for j in listing3:
                    listing4 = os.listdir(directory + str(i) + '/' + str(k) + '/' + str(j))
                    for l in listing4:
                        f.write(directory + str(i) + '/' + str(k) + '/' + str(j) + '/' + str(l))
                        f.write('\n')


def first_scan(pathlist):
    with open('datalist_COD.csv', 'a') as datalist:  # we create a csv file to write info into
        datalist.write("COD_ID,path,pretty_formula,compound,lattice_system,spacegroup,species,volume_cell,mag_sites,comment1,doi\n")
    no_pretty_formula_counter = 0
    total_compounds = len(open(pathlist).readlines())
    with tqdm.tqdm(total=total_compounds) as pbar:        # A wrapper that creates nice progress bar
        pbar.set_description("Processing")
        with open(pathlist, 'r') as f:  # need to get POSCAR content from big structure file
            for path in f:
                pbar.update(1)
                with open(path.strip('\n'), 'r') as cif:
                    content = cif.read()
                    try:
                        pretty_formula = content.split('_chemical_formula_sum')[1].split('\n')[0].strip(' ').strip("'").strip()
                    except:
                        pretty_formula = 'na'
                        no_pretty_formula_counter = no_pretty_formula_counter + 1
                        pass
                    try:
                        elemets = pretty_formula.translate({ord(i): None for i in '1234567890'}).split()
                    except:
                        elemets = 'na'
                        pass

                    necessary = ["Mn", "Fe", "Si", "Ni"," Y", "Zr", "Al", "Cu"] # at least one of these must be present
                    banlist = ["Re", "Os", "Ir", "Pt", "Au", "In", "Tc",  # Expensive or Limited in supply
                               "Be", "As", "Cd", "Ba", "Hg", "Tl", "Pb", "Ac",  # Health Hazard
                               "Cs", "Po", "Np", "U", "Pu", "Th",  # Radioactive
                               "He", "Ne", "Ar", "O","Kr", "Xe"]  # Noble gases

                    necessary_match = [i for i in necessary if i in elemets]
                    if necessary_match:
                        banlist_match = [i for i in banlist if i in elemets]

                        if not banlist_match:
                            try:
                                COD_id = content.split('data_')[1].split('\n')[0].strip(' ')
                            except:
                                COD_id = 'na'
                            try:
                                compound = content.split('_chemical_formula_structural')[1].split('\n')[0].strip(' ').strip("'")
                            except:
                                compound = 'na'
                            try:
                                lattice_system = content.split('_symmetry_cell_setting')[1].split('\n')[0].strip(' ').strip("'")
                            except:
                                lattice_system = 'na'
                            try:
                                spacegroup = content.split('_space_group_IT_number')[1].split('\n')[0].strip(' ').strip("'")
                            except:
                                spacegroup = 'na'

                            try:
                                volume = content.split('_cell_volume')[1].split('\n')[0].strip(' ').strip("'")
                            except:
                                volume = 'na'
                            try:
                                doi = content.split('_journal_paper_doi')[1].split('\n')[0].strip(' ').strip("'")
                            except:
                                doi = 'na'
                            try:
                                comment1 = content.split('_publ_section_title')[1].split('\n')[2].split('_journal_name_full')[0]
                            except:
                                comment1 = 'na'

                            # Now we write all parsed results into .csv file

                            newrow = str(
                                COD_id) + ',' + str(
                                path.strip('D:/').strip('\n')) + ',' + str(
                                pretty_formula) + ',' + str(
                                compound) + ',' + str(
                                lattice_system) + ',' + str(
                                spacegroup) + ',' + str(
                                elemets).replace(',', ';') + ',' + str(
                                volume) + ',' + str(
                                0) + ',' + str(
                                comment1).replace(',', ';') + ',' + str(
                                doi).strip(',') + ',' + '\n'
                            with open('datalist_COD.csv', 'a') as datalist:
                                datalist.write(newrow)

    print('No chemical composition given in cif for ', no_pretty_formula_counter, ' entries')


def reorganize(datalist, destination):
    df = pd.read_csv(datalist, index_col=0, sep=',', low_memory=False)
    with tqdm.tqdm(total=len(df.index)) as pbar:  # A wrapper that creates nice progress bar
        pbar.set_description("Processing datalist")
        for item in df.index.tolist():
            pbar.update(1)  # Updating progress bar at each step
            source = 'D:/' + df.loc[item, 'filepath']
            # with open('testlist', "a") as f:
            #     f.write(str(source)+'\n')
            shutil.move(source, destination)



dest = 'D:/MCES/COD/datadir'
wdatalist = 'datalist_COD.csv'



# first_scan('COD_all_paths')

# wdatadir_structure = 'D:/'
#
# reorganize(wdatalist, dest)
#
# df = pd.read_csv(wdatalist, index_col=0, sep=',', low_memory=False)
# source = df.loc['1000094', 'filepath']
# print(source)
