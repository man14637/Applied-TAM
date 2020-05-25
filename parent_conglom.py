'''
Created on Nov 27, 2019

@author: michael
'''
import os
import dbf
from dbf import READ_ONLY

def main():
    par_csr_list = {}
    cong_par_list = {}
    cli_csr_list = {}
    comb_csr_list = {}
    comp_csr = []
    
    con_list(par_csr_list)
    for k,v in sorted(par_csr_list.items()):
        comp_csr.append(k)
    print (len(comp_csr))
    print(len(par_csr_list))
    input("...Done With Parent....")
    
    csr_list(par_csr_list, cli_csr_list)
    
    for k,v in sorted(cli_csr_list.items()):
        if k in comp_csr:
            continue
        else:
            comp_csr.append(k)
    print (len(comp_csr))
    comp_csr = sorted(comp_csr)
    print ('CSR count of Policies not parent: ' + str(len(cli_csr_list)))
    input('.....Done with Non-Conglom/Parent......')
    
    comb_csr(comp_csr,par_csr_list,cli_csr_list,comb_csr_list)
    cli_csr_list.clear()
    print ('CSR count all policies: ' + str(len(comb_csr_list)))
    input('.....Done with Combing CSRs......')
    
    cong_list(par_csr_list,cong_par_list)
    print (len(cong_par_list))
    input("...Done With Conglom....")
    
    
def comb_csr(comp_csr,par_csr_list,cli_csr_list,comb_csr_list):
    x=1
    int(x)
    p = len(comp_csr)
    
    for x in range (0,int(p)):
        val_list =[]
        val1_list =[]
        csr = comp_csr[x]
        
        val_list = par_csr_list.get(csr)
        val1_list = cli_csr_list.get(csr)

        if val_list is None:
            val_list = sorted(val1_list)
            comb_csr_list.setdefault(csr,[]).append(val_list)
        elif val1_list is None:
            val_list = sorted(val_list)
            comb_csr_list.setdefault(csr,[]).append(val_list)
        else:
            val_list = sorted((val_list + val1_list)) 
            comb_csr_list.setdefault(csr,[]).append(val_list)
        

#Creating a Date conversion function
def con_date(d):
    if d is 'A':
        return '200'
    elif d is 'B':
        return '201'
    elif d is 'C':
        return '202'
    elif d is 'D':
        return '203'
    elif d is 'E':
        return '204'
    elif d is 'F':
        return '205'
    elif d is 'G':
        return '206'
    elif d is 'H':
        return '207'
    elif d is 'I':
        return '208'
    elif d is 'J':
        return '209'
    else:
        return ''
# end of Con_date

def csr_lookup(csrlu):
    os.chdir("G:\\TAM\\Data")
    flds_table = dbf.Table("FLDS.DBF")
    flds_table.open(mode=READ_ONLY)
    flds_table.use_deleted = False
    
    for record in flds_table:
        if record.fld == 1 and record.value == csrlu:
            return csrlu == record.desc
            flds_table.close()
            break
        
def pr_lookup(prlu):
    os.chdir("G:\\TAM\\Data")
    ins_table = dbf.Table("INS.DBF")
    ins_table.open(mode=READ_ONLY)
    ins_table.use_deleted = False
    
    for record in ins_table:
        if record.key == "P" and record.rec == prlu:
            return prlu == record.name
            ins_table.close()
            break

def con_list(par_csr_list):
    ## collecting all Parent Clients in TAM associated to CSR
    os.chdir("D:\\TAM\\Data")
    pol_table = dbf.Table("POLICY.DBF")
    pol_table.open(mode=READ_ONLY)
    pol_table.use_deleted = False
    
    for record in pol_table:
        ##Attributes
        par_list = []
        client = (record.pol_idx[0:7]).strip()
        exp = "".join((record.exp).strip())
        if client =='.PICTUR' or exp == '':
            continue
        else:
            mth = record.exp[:2]
            mth = int(mth)
            yre = record.exp[7:8]
            yr = record.exp[6:7]
            yrs = con_date(yr) + yre
            yrs = int(yrs)
            csr = (record.csr).strip()
            cong = (record.conglom).strip()
            
            
            if (record.agcy != 3 and record.agcy != 4) and ((yrs == 2019 and mth >= 11) or yrs >= 2020)and record.code == 'CL':
                if cong =='Parent' and csr in par_csr_list:
                    par_list = par_csr_list[csr]
                    
                    if client in par_list:
                        continue
                    elif client not in par_list:
                        par_csr_list.setdefault(csr,[]).append(client)
                        
                elif cong =='Parent':
                    par_csr_list.setdefault(csr,[]).append(client)
    pol_table.close()
    
def cong_list(par_csr_list,cong_par_list):
    ## collecting all Conglom Clients in TAM associated to Parent
    os.chdir("D:\\TAM\\Data")
    pol_table = dbf.Table("POLICY.DBF")
    pol_table.open(mode=READ_ONLY)
    pol_table.use_deleted = False
    
    x = 0
    int(x)
    p = len(par_csr_list)
    
    for x in range (0,int(p)):  
        par_list = []
        par_list = [value for value in par_csr_list.values()][x]
        
        for record in pol_table:
            ##Attributes
            client = (record.pol_idx[0:7]).strip()
            exp = "".join((record.exp).strip())
            if client =='.PICTUR' or exp == '':
                continue
            else:
                mth = record.exp[:2]
                mth = int(mth)
                yre = record.exp[7:8]
                yr = record.exp[6:7]
                yrs = con_date(yr) + yre
                yrs = int(yrs)
                cong = (record.conglom).strip()
            
                if (record.agcy != 3 and record.agcy != 4) and ((yrs == 2019 and mth >= 12) or yrs >= 2020) and (cong in par_list):
                    if cong in cong_par_list:
                        chk_par =[]
                        chk_par = cong_par_list[cong]
                        
                        if client in chk_par:
                            continue
                        else:
                            cong_par_list.setdefault(cong,[]).append(client)
                        
                        
                    else:
                        cong_par_list.setdefault(cong,[]).append(client)
    
    pol_table.close()
    
def csr_list (par_csr_list, cli_csr_list):
    ## collecting all non-Conglom/Parent Clients in TAM
    os.chdir("D:\\TAM\\Data")
    pol_table = dbf.Table("POLICY.DBF")
    pol_table.open(mode=READ_ONLY)
    pol_table.use_deleted = False
    
    for record in pol_table:
        ##Attributes
        client = (record.pol_idx[0:7]).strip()
        exp = "".join((record.exp).strip())
        if client =='.PICTUR' or exp == '':
            continue
        else:
            mth = record.exp[:2]
            mth = int(mth)
            yre = record.exp[7:8]
            yr = record.exp[6:7]
            yrs = con_date(yr) + yre
            yrs = int(yrs)
            csr = (record.csr).strip()
            cong = (record.conglom).strip()
            
            if (record.agcy != 3 and record.agcy != 4) and ((yrs == 2019 and mth >= 12) or yrs >= 2020) and (cong != 'Parent') and record.code == 'CL':
                pa_list = []
                pa_list = par_csr_list.get(csr)
                ch_list = []
                ch_list = cli_csr_list.get(csr)
                
                if pa_list is None:
                    if ch_list is None:
                        cli_csr_list.setdefault(csr,[]).append(client)
                    elif client in ch_list:
                        continue
                    else:
                        cli_csr_list.setdefault(csr,[]).append(client)
                elif cong in pa_list:
                    continue
                elif ch_list is None:
                    cli_csr_list.setdefault(csr,[]).append(client)
                elif client in ch_list:
                    continue
                else:
                    cli_csr_list.setdefault(csr,[]).append(client)
                    
    pol_table.close()
    
    
main()