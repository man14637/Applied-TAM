'''
Created on Jul 25, 2019

@author: michael
'''
import os
import dbf
import csv
from dbf import READ_ONLY
import re

def main():
    #Declaring main variables CSR/Drive
    bco_csv = input("Please CSV NAME: ")
    bco = dict()
    co_csv(bco_csv,bco)
    pol(bco)
    
##reading csv of Company Codes and Names
def co_csv(bco_csv,bco):
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    with open(bco_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            bco[row[0]] = row[1].replace(',','')
            line_count += 1
        print (line_count)
#Creating a Date conversion function

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
def ins(nam):
    ## nam needs to be a dictionary function
    insTable = dbf.Table("INS")
    insTable.open(mode=READ_ONLY)
    insTable.use_deleted = False
    
    for record in insTable:
        if record.key == 'C' and (record.agcy != 3 and record.agcy != 4):
            cus = record.rec.strip()
            cus_name = record.name.replace(',','')
            cus_name = cus_name.strip()
            nam[cus] = cus_name
        else:
            continue
    insTable.close()
    return nam
    
#Getting Code descriptions for Policy types
def fld(f):
    ## make sure f is dict() 
    flds = dbf.Table("FLDS")
    flds.open(mode=READ_ONLY)
    flds.use_deleted = False
    
    for record in flds:
        if record.fld == 'B':
            val = record.value.replace(' ','')
            val.strip()            
            desc = str(record.desc)
            desc = desc.strip()
            f[val] = desc
        else:
            continue
    flds.close()
    return f
# end of flds


def pol(bco):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")

    table = dbf.Table("POLICY")
    table.open(mode=READ_ONLY)
    table.use_deleted = False
    
    ##Declaring Variables
    cl = dict()
    pl = dict()
    cl_list=[]
    pl_list=[]
    f_name = 'policy_location.csv'
    fl = dict()
    fld(fl)
    
    nam = dict()
    ins(nam)
    
    print ("Policy Database Size: %d" % table.__len__())
    
    for record in table:
        ##Attributes
        exp_ck = record.exp [6:7]
        mth = record.exp[:2]
        yre = record.exp[7:8]
        yr = record.exp[6:7]
        yrs = con_date(yr) + yre
        eff = record.eff[0:6] + yrs
        exp = record.exp[0:6] + yrs
        
        if record.bco in bco and (record.agcy != 3 and record.agcy != 4) and not(exp_ck == 'A' or exp_ck.isdigit()) and int(yrs) >= 2019:
            if int(yrs) == 2019 and int(mth) < 8:
                continue
            else:
                client = record.pol_idx[:9].upper()
                pol_num = int(record.pol_idx[11:12])
                pol_type = record.type.replace(" ","")
                pol_type = fl[pol_type]
                pol = record.pol.strip()
                
                if record.typegroup == 'CO':                    
                    if client in cl:
                        if pol_num == 1:
                            pol_types = cl[client]
                            del cl[client]
                            cl[client] =(pol + ',' + record.bco +','+ bco[record.bco] +','+record.ico + ',' + eff + ',' + exp + ',' + str(record.prem) + ',' + pol_type + ',' + pol_types)
                        else:
                            pol_ty = cl[client] + ',' + pol_type
                            cl[client] = pol_ty
                    else:
                        if pol_num == 1:
                            cl[client] =(pol + ',' + record.bco +','+ bco[record.bco] +','+ record.ico + ',' + eff + ',' + exp + ',' + str(record.prem) + ',' + pol_type)
                        else:
                            cl[client] = pol_type
                
                elif record.typegroup == 'PL':
                    if client in pl:
                        if pol_num == 1:
                            pol_types = pl[client]
                            del pl[client]
                            pl[client] = (pol + ',' + record.bco +','+ bco[record.bco] +','+ record.ico + ',' + eff + ',' + exp + ',' + str(record.prem) + ',' + pol_type + ',' + pol_types)
                        else:
                            pol_ty = pl[client] + ',' + pol_type
                            pl[client] = pol_ty
                    else:
                        if pol_num == 1:
                            pl[client] = (pol + ',' + record.bco +','+ bco[record.bco] +','+ record.ico + ',' + eff + ',' + exp + ',' + str(record.prem) + ',' + pol_type)
                        else:
                            pl[client] = pol_type
                else:
                    continue
    table.close()
       
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Reports")
    
    print ('Amount of CL clients: %d' %len(cl))
    print ('Amount of PL clients: %d' %len(pl))
    
    with open(f_name , 'w') as f:
        f.write('REC,NAME,POL,BCO,BCO_Name,ICO,EFF,EXP,PREM,TYPE,type2,type3,type4,type5,type6\n')
        for key in cl.keys():
            rec1 = key[0:7]
            rec_name = nam[rec1]
            rec_name = rec_name.strip()
            f.write("%s,%s,%s\n"%(key,rec_name,cl[key]))
            
            ## record splitting/matching app data
            rec2 = key[7:9]
            rec3 = rec1 + 'C' + rec2
            cl_list.append(rec3)
            
    f.close()
    cl.clear()
    
    with open(f_name, 'a') as p:
        for key in pl.keys():
            re1 = key[0:7]
            p.write("%s,%s,%s\n"%(key,nam[rec1],pl[key]))
            
            ## record splitting/matching app data
            re2 = key[7:9]
            re3 = re1 + 'C' + re2
            pl_list.append(re3)
    p.close()
    pl.clear()
    input("Policy Data written to CSV......")
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    
    cl_ap = dict()
    pl_ap = dict()
    
    cl_app(cl_list,cl_ap)
    pl_app(pl_list,pl_ap)
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Reports")
    
    with open('cl_whsale.csv', 'w') as c:
        c.write('REC,PolicyREC,Address,Year,PremDesc1\n')
        for key in cl_ap.keys():
            c.write("%s,%s\n"%(key,cl_ap[key]))
    c.close()
    
    with open('pl_whsale.csv' , 'w') as p:
        p.write('REC,PolicyREC,Name,Year,Street,City,State,Zip\n')
        for key in pl_ap.keys():
            p.write("%s,%s\n"%(key,pl_ap[key]))
    p.close()
    input("PL and CL App Data written to CSV......")
    
#Closing INS Database and DBF

def cl_app(cl_list,cl_ap):
    #Opening Commercial Lines App Database
    table = dbf.Table("AP1DB1B")
    table.use_deleted = False
    table.open(mode=dbf.READ_WRITE)
    
    print ("AP1 Database Size: %d" % table.__len__())
    print('Length of CL clients: %d' % len(cl_list))
    
    for record in table:
        rec_pol = record.rec[0:10]
        
        ## Getting Year Built
        yr_built = record.preminfo.strip()
        yr_built = yr_built.replace(" ","")
        yr_built = yr_built[0:4]

        if record.key == 'P' and (rec_pol in cl_list):
            re = rec_pol[0:7]
            re1 = rec_pol[8:10]
            re2 = re + re1
            cl_ap[record.rec] = (re2 +','+ record.address.replace(",","") +','+ yr_built + ',' + record.premdesc1.replace(",","/"))    
        else:
            continue  
    table.close()
    return cl_ap

def pl_app(pl_list,pl_ap):
        
    #Opening Personal Lines App Database
    table2 = dbf.Table("AP2DBF")
    table2.use_deleted = False
    table2.open(mode=dbf.READ_WRITE)
    
    print ("AP2 Database Size: %d" % table2.__len__())
    print('Length of PL clients: %d' % len(pl_list))
    
    
    for record in table2:
        
        ## Getting Year Built
        pl_YrBuilt = record.chome20.strip()
        pl_YrBuilt = pl_YrBuilt.replace(" ","")
        pl_YrBuilt = pl_YrBuilt[0:4]
        
        rec_pol = record.rec[:10]
        if rec_pol in pl_list:
            re = rec_pol[0:7]
            re1 = rec_pol[8:10]
            re2 = re + re1
            pl_ap[record.rec] = (re2 +','+ record.name.replace(",","") +','+ pl_YrBuilt +','+ record.street.replace(",","") + ' ' + record.city.replace("","") + ' ' + record.st + ' ' + record.zip)
        else:
            continue
    table2.close()
    return pl_ap
    #Closing AP2 database

main()
