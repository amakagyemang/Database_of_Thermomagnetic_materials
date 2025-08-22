from mysql.connector import connect, Error
# from time import sleep
import tqdm

out_path = '/scratch/agyemang/screening_test/Downloader'

with open('ids_to_download.txt', 'r') as id_file:
    Lines = id_file.readlines()
    with tqdm.tqdm(total=len(Lines)) as pbar:        # A wrapper that creates nice progress bar

        for line in Lines:
            pbar.update(1)
            icsdid = line.strip('\n')
            # sleep(0.5)
            try:
                with connect(
                    host="mysql-icsd.science.ru.nl",
                    user="icsd_reader",
                    password="ePnV3Od6u0rbQUVa",
                    database="icsd",
                ) as connection:
                    select_query = """
            SELECT
            icsd.idnum,
            icsd.coll_code,
            icsd.rec_date,
            icsd.chem_name,
            icsd.struct_form,
            icsd.sum_form,
            coden.j_title,
            reference.year,
            reference.volume,
            reference.page_first,
            reference.page_last,
            reference.coden,
            icsd.authors_text,
            icsd.a_text,
            icsd.b_text,
            icsd.c_text,
            icsd.alpha_text,
            icsd.beta_text,
            icsd.gamma_text,
            icsd.c_vol,
            icsd.z,
            space_group.sgr_disp,
            space_group.sgr_num,
            icsd.r_val,
            p_record.p_seq,
            p_record.el_symbol,
            p_record.el_label,
            p_record.ox_state,
            p_record.w_mult,
            p_record.w_lett,
            p_record.x_text,
            p_record.y_text,
            p_record.z_text,
            p_record.itf_text,
            p_record.sof_text,
            p_record.tf11_text,
            p_record.tf22_text,
            p_record.tf33_text,
            p_record.tf12_text,
            p_record.tf13_text,
            p_record.tf23_text,
            symmetry_records.sgr,
            symmetry_records.sym_seq,
            symmetry_records.r11,
            symmetry_records.r12,
            symmetry_records.r13,
            symmetry_records.t1,
            symmetry_records.r21,
            symmetry_records.r22,
            symmetry_records.r23,
            symmetry_records.t2,
            symmetry_records.r31,
            symmetry_records.r32,
            symmetry_records.r33,
            symmetry_records.t3,
            icsd.au_title,
            space_group.smat_genrpos,
            p_record.atf_code
            FROM icsd
            LEFT JOIN reference ON icsd.idnum=reference.idnum
            LEFT JOIN icsd_remarks ON icsd.idnum=icsd_remarks.idnum
            LEFT JOIN icsd_tests ON icsd.idnum=icsd_tests.idnum
            LEFT JOIN comments ON icsd.idnum=comments.idnum
            LEFT JOIN p_record ON icsd.idnum=p_record.idnum
            LEFT JOIN coden ON reference.coden=coden.coden
            LEFT JOIN space_group ON icsd.sgr=space_group.sgr
            LEFT JOIN symmetry_records ON icsd.sgr=symmetry_records.sgr
            WHERE icsd.idnum IN (""" + str(icsdid) + """) ORDER BY icsd.idnum desc, p_record.p_seq;
            """
                    with connection.cursor() as cursor:
                        cursor.execute(select_query)
                        records = cursor.fetchall()
                        # print("Total rows are:  ", len(records))
                        row = records[0]

            ################################################
                        with open(out_path + icsdid + '.cif', "w") as cif_file:
                            atom = ''
                            aniso = ''
                            atf_disp = ''
                            idnum = row[0]
                            ccode = str(row[1]).strip('\n')
                            adate = str(row[2]).strip('\n')
                            cvol = str(round(float(row[19]), 2))
                            if row[23] != None: rfac = round(float(row[23]), 2)
                            sgrf = row[41]
                            sgrd = str(row[21]).replace(' Z', '').replace(' S', '').replace(' H', '')
                            ref = (row[6].encode('utf-8')).strip('\n') + ' ' + str(row[8]).strip('\n') + ', ' + str(row[9]).strip('\n') + '-' + str(row[10])
                            aut = row[12].encode('utf-8')
                            authors = aut.replace('# ', '')
                            tit = row[55].encode('utf-8')
                            title = tit.replace('# ', '')
                            sformula = str(row[4]).replace(' ', '')
                            journal = row[6].encode('utf-8')[0:40]
                            pseq = int(str(row[24]).strip('\n'))

                            cif_file.write("###############################################################################\n")
                            cif_file.write("# " + aut.strip('\n') + ' ' + str(row[7]) + '\n')
                            cif_file.write("# " + ref + '\n')
                            cif_file.write("# " + title + '\n')
                            cif_file.write("# ICSD_ID: " + icsdid + "\n")
                            cif_file.write("# CIF parser for ICSD. Part of MCEScreener package by I.Batashev ik.batashev@physics.msu.ru \n")
                            cif_file.write("# NOT TO BE PUBLISHED IN ANY FORM.\n")
                            cif_file.write("###############################################################################\n")
                            cif_file.write("\n")
                            cif_file.write("data_" + ccode + "-ICSD\n")
                            cif_file.write("_audit_creation_date               " + str('"') + adate + str('"') + '\n')
                            cif_file.write("_chemical_name_systematic\n")
                            cif_file.write(str('"') + row[3].encode('utf-8') + str('"') + '\n')
                            cif_file.write("_chemical_formula_structural\n")
                            cif_file.write(str('"') + row[4].encode('utf-8') + str('"') + '\n')
                            cif_file.write("_publ_section_title\n")
                            cif_file.write(str('"') + row[5].encode('utf-8') + str('"') + '\n')
                            cif_file.write("loop_\n")
                            cif_file.write("_citation_id\n")
                            cif_file.write("_citation_journal_abbrev\n")
                            cif_file.write("_citation_year\n")
                            cif_file.write("_citation_journal_volume\n")
                            cif_file.write("_citation_page_first\n")
                            cif_file.write("_citation_page_last\n")
                            cif_file.write("_citation_journal_id_ASTM\n")
                            cif_file.write("primary " + str('"') + journal.strip('\n') + str('"') + " " + str(row[7]).strip('\n') + " " + str(row[8]).strip('\n') + " " + str(row[9]).strip('\n') + " " + str(row[10]).strip('\n') + " " + str(row[11]) + '\n')
                            cif_file.write("loop_\n")
                            cif_file.write("_publ_author_name\n")
                            cif_file.write(str('"') + authors + str('"') + "\n")

                            cif_file.write("_cell_length_a                     " + str(row[13]) + '\n')
                            cif_file.write("_cell_length_b                     " + str(row[14]) + '\n')
                            cif_file.write("_cell_length_c                     " + str(row[15]) + '\n')
                            cif_file.write("_cell_angle_alpha                  " + str(row[16]) + '\n')
                            cif_file.write("_cell_angle_beta                   " + str(row[17]) + '\n')
                            cif_file.write("_cell_angle_gamma                  " + str(row[18]) + '\n')
                            cif_file.write("_cell_volume                       " + str(cvol) + '\n')
                            cif_file.write("_cell_formula_units_Z              " + str(row[20]) + '\n')
                            cif_file.write("_symmetry_space_group_name_H-M     " + str('"') + str(sgrd) + str('"') + '\n')
                            cif_file.write("_symmetry_Int_Tables_number        " + str(row[22]) + '\n')
                            if row[23] != None: cif_file.write("_refine_ls_R_factor_all            " + str(rfac) + "\n")
                            cif_file.write("loop_\n")
                            cif_file.write("_symmetry_equiv_pos_site_id\n")
                            cif_file.write("_symmetry_equiv_pos_as_xyz\n")
                            cif_file.write(row[56] + "\n")
                            cif_file.write("loop_\n")
                            cif_file.write("_atom_site_type_symbol\n")
                            cif_file.write("_atom_site_label\n")
                            cif_file.write("_atom_site_symmetry_multiplicity\n")
                            cif_file.write("_atom_site_Wyckoff_symbol\n")
                            cif_file.write("_atom_site_fract_x\n")
                            cif_file.write("_atom_site_fract_y\n")
                            cif_file.write("_atom_site_fract_z\n")
                            cif_file.write("_atom_site_occupancy\n")
                            atom = ''
                            for row in records:
                                occup = str(row[34]).strip('\n')
                                if occup == 'None':
                                    occup = '1.0'
                                next_atom = str(row[25]).strip('\n') + ' ' + str(row[25]).strip('\n') + str(row[26]).strip('\n') + ' ' + str(row[28]).strip('\n') + ' ' + str(row[29]).strip('\n') + ' ' + str(row[30]).strip('\n') + ' ' + str(row[31]).strip('\n') + ' ' + str(row[32]).strip('\n') + ' ' + occup
                                if next_atom != atom:
                                    atom = next_atom
                                    cif_file.write(atom + "\n")
                                else:
                                    pass

                                # if str(row[30]) != '0' and str(row[31]) != '0' and str(row[32]) != '0':

            ################################################
                    connection.close()

            except Error as e:
                print(e)
