# https://rosenbrockc.github.io/aflow/control.html
# https://rosenbrockc.github.io/aflow/entries.html
# https://www.researchgate.net/publication/320179929_A_Practical_Python_API_for_Querying_AFLOWLIB
from aflow import *
import time

result = search(catalog='icsd', batch_size=100
    ).filter(
             ### Must contain at least on of these:
             ((K.species == "Mn") |
              (K.species == "Fe") |
              (K.species == "Al") |
              (K.species == "Zr") |
              (K.species == "Si") |
              (K.species == "Ni") |
              (K.species == "Cu"))
             &
             ### Does not contain any of these because:
             ### Expensive or limited in supply:
             (K.species != "Re") &
             (K.species != "Os") &
             (K.species != "Ir") &
             (K.species != "Pt") &
             (K.species != "Au") &
             (K.species != "In") &
             ### Health Hazard:
             # is Tc in Aflow?
             (K.species != "Be") &
             (K.species != "As") &
             (K.species != "Cd") &
             (K.species != "Cs") &
             (K.species != "Ba") &
             (K.species != "Hg") &
             (K.species != "Tl") &
             (K.species != "Pb") &
             ### Noble gases:
             # (K.species != "He") & only reason He is not sorted out is because i reached maximum possible querry criteria,
             # and there aren't any compounds with He anyway so it is safe to leave and include one more important instead
             (K.species != "Ne") &
             (K.species != "Ar") &
             (K.species != "Kr") &
             (K.species != "Xe")
             #  ~~Elements afte Bi not mentioned this list are not in aflow database at all. contains only first 83 elements, so they are sorted out by default.~~
             #  Unfortunately U, Pu and Th are there despite no mention of Aflow GUI :(
             #  maybe something else is also possible in database wider than ICSD
      ).filter(K.files == "CONTCAR.relax.vasp"
             # This is only to look at entries that have POSCAR.
             # This selection is not really necessary as it seems that all entries in FULL aflolib have POSCAR.relax structure file
             # (165705 hits with; same 165705 hits without)
      ).filter((K.spin_cell > 0) | (K.spin_cell < 0)
             # only find entries with non-zero moment
      ).filter(K.enthalpy_cell < 0)
             # entalpy must me negative - othervise structure would not form in real life

totalN = len(result)
print(totalN)

def downloader(counter=0, default_decounter=500):
    """Function used to download files and fill up datalist with information from Aflow
    Works with search result, has two parameters:
    counter - The search entry where we start downloading from (technically we always want zero, only added for flexebility)
    decounter - number of entries we download in a batch before waiting for some time in order not to overburden aflow with requests"""

    with open('./datalist_more_info_check.csv', 'a') as f:        # we create a csv file to write info into
        f.write("ID,aflow_ID,compound,energy_cell,energy_atom,lattice_system,Bravais_lattice,original_Bravais_lattice,spacegroup,geometry,species,volume_cell,moment_cell,mag_field,mag_sites,comment1,comment2\n")
    decounter = default_decounter
    while counter <= totalN:
        result[counter].files["edata.relax.out"]("./downloaded_data_structure_relaxed/"+str(counter))  # Structural informaion file after relaxation
        # result[counter].files["edata.orig.out"]("./downloaded_data_structure/"+str(counter))           # Structural information file before relaxation (very optional we only need a single structure file)
        # result[counter].files["INCAR.bands"]("./downloaded_data_incars/" + str(counter))             # INCAR file not necessary for screening, optional download to gather more info on calculations done by aflolib
        result[counter].files["aflowlib.out"]("./downloaded_data_aflow/" + str(counter))               # File containing ALL information on entry, same (but less tags) info goes into datalist so optional, but useful download
        newrow = str(counter)+','+str(result[counter].auid)+','+ str(result[counter].compound)+','+ str(result[counter].energy_atom)+','+ str(result[counter].energy_cell)+','+str(result[counter].lattice_system_relax.strip('\n'))+','+str(result[counter].Bravais_lattice_relax.strip('\n'))+','+str(result[counter].Bravais_lattice_orig.strip('\n'))+','+ str(result[counter].spacegroup_relax)+','+ str(result[counter].geometry).replace(',', ';')+','+str(result[counter].species).replace(',', ';')+','+str(result[counter].volume_cell)+','+ str(result[counter].spin_cell)+','+'0'+','+'0'+','+''+','+'\n'
        with open('./datalist_more_info_check.csv', 'a') as f:
            f.write(newrow)
        if decounter == 0:
            print(counter, '   ', (counter/totalN)*100, '% done') # just a simple progress indicator (may try some fancy stuff later...)
            time.sleep(300)
            decounter = default_decounter
        counter = counter + 1
        decounter = decounter - 1

# downloader(7687,500)
