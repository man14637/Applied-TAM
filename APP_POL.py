'''
Created on Jul 16, 2019

@author: michael
'''
import os
import dbf
from dbf import READ_ONLY

def main():
    #Declaring main variables CSR/Drive
    mco = input("Please input 3 character MCO code: ")
    mco.upper()
    pol(mco)

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

#Getting Code descriptions for Policy types
def fld(f):
    
    flds = dbf.Table("FLDS")
    flds.open(mode=READ_ONLY)
    flds.use_deleted = False
    cod_legth = len(f)
    for record in flds:
        val = str(record.value)
        val = val[0:cod_legth]
        
        if record.fld == 'B' and val == f:
            desc = str(record.desc)
            flds.close()
            return desc
        else:
            continue
# end of flds

def pol(mco):
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
    f_name = mco +'_policy.csv'
    
    print ("Policy Database Size: %d" % table.__len__())
    
    for record in table:
        ##Attributes
        exp_ck = record.exp [6:7]
        mth = record.exp[:2]
        yre = record.exp[7:8]
        yr = record.exp[6:7]
        yrs = con_date(yr) + yre
        
        if record.mco == mco and (record.agcy != 3 and record.agcy != 4) and not(exp_ck == 'A' or exp_ck.isdigit()) and int(yrs) >= 2018:
            if int(yrs) == 2018 and int(mth) < 7:
                continue
            else:
                client = record.pol_idx[:9].upper()
                pol_num = int(record.pol_idx[11:12])
                
                eff = record.eff[0:6] + yrs
                exp = record.exp[0:6] + yrs
                
                if record.typegroup == 'CO':
                    pol_type = record.type.replace(" ","")
                    pol_type = fld(pol_type)
                    
                    if client in cl:
                        if pol_num == 1:
                            pol_types = cl[client]
                            del cl[client]
                            cl[client] =(record.bco +','+ record.ico + ',' + eff + ',' + exp + ',' + str(record.prem) + ',' + pol_type + ',' + pol_types)
                        else:
                            pol_ty = cl[client] + ',' + pol_type
                            cl[client] = pol_ty
                    else:
                        if pol_num == 1:
                            cl[client] =(record.bco +','+ record.ico + ',' + eff + ',' + exp + ',' + str(record.prem) + ',' + pol_type)
                        else:
                            cl[client] = pol_type
                else:
                    continue
    table.close()
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Reports")
    
    print ('Amount of CL clients: %d' %len(cl))
    
    
    with open(f_name , 'w') as f:
        f.write('REC,BCO,ICO,EFF,EXP,PREM,TYPE,type2,type3,type4,type5,type6\n')
        for key in cl.keys():
            f.write("%s,%s\n"%(key,cl[key]))
            ## record splitting
            rec1 = key[0:7]
            rec2 = key[7:9]
            rec3 = rec1 + 'C' + rec2
            cl_list.append(rec3)
    f.close()
    cl.clear()
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    
    table2 = dbf.Table("POLICY")
    table2.open(mode=READ_ONLY)
    table2.use_deleted = False
    
    for record in table2:
        exp_ck = record.exp [6:7]
        mth = record.exp[:2]
        yre = record.exp[7:8]
        yr = record.exp[6:7]
        yrs = con_date(yr) + yre
        
        if record.mco == mco and (record.agcy != 3 and record.agcy != 4) and not(exp_ck == 'A' or exp_ck.isdigit()) and int(yrs) >= 2018:
            if int(yrs) == 2018 and int(mth) < 7:
                continue
            else:
                client = record.pol_idx[:9].upper()
                pol_num = int(record.pol_idx[11:12])

                eff = record.eff[0:6] + yrs
                exp = record.exp[0:6] + yrs
                
                if record.typegroup == 'PL':
                    pol_type = record.type.replace(" ","")
                    pol_type = fld(pol_type)
                    
                    if client in pl:
                        if pol_num == 1:
                            pol_types = pl[client]
                            del pl[client]
                            pl[client] = (record.bco +','+ record.ico + ',' + eff + ',' + exp + ',' + str(record.prem) + ',' + pol_type + ',' + pol_types)
                        else:
                            pol_ty = pl[client] + ',' + pol_type
                            pl[client] = pol_ty
                    else:
                        if pol_num == 1:
                            pl[client] = (record.bco +','+ record.ico + ',' + eff + ',' + exp + ',' + str(record.prem) + ',' + pol_type)
                        else:
                            pl[client] = pol_type
                else:
                    continue
    table2.close()
    
    print ('Amount of PL clients: %d' %len(pl))
   
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Reports")
    
    with open(f_name, 'a') as p:
        for key in pl.keys():
            p.write("%s,%s\n"%(key,pl[key]))
            re1 = key[0:7]
            re2 = key[7:9]
            re3 = re1 + 'C' + re2
            pl_list.append(re3)
    p.close()
    pl.clear()
    
    input("Policy Data written to CSV......")
    app(cl_list,pl_list,mco)
#Closing INS Database and DBF

def app(cl_list,pl_list,mco):
    #Reading DBF file / Changing to DBF Directory
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    
    #Opening Commercial Lines App Database
    table = dbf.Table("AP1DB1B.DBF")
    table.use_deleted = False
    table.open(mode=dbf.READ_WRITE)
    
    print ("AP1 Database Size: %d" % table.__len__())
    print('Length of CL clients: %d' % len(cl_list))
    
    cl_app = dict()
    pl_app = dict()
    c_name = mco +'_CLApp.csv'
    p_name = mco +'_PLApp.csv'
    
    for record in table:
        rec_pol = record.rec[:10]

        if record.key == 'P' and (rec_pol in cl_list):
            re = rec_pol[0:7]
            re1 = rec_pol[8:10]
            re2 = re + re1
            cl_app[record.rec] = (re2 +','+ record.address.replace(",","") +','+ record.preminfo.replace(",","/") + ',' + record.premdesc1.replace(",","/"))    
        else:
            continue    
    table.close()
    #Closing AP1 database
    
    #Opening Personal Lines App Database
    table2 = dbf.Table("AP2DBF.DBF")
    table2.use_deleted = False
    table2.open(mode=dbf.READ_WRITE)
    
    print ("AP2 Database Size: %d" % table2.__len__())
    print('Length of PL clients: %d' % len(pl_list))
    
    for record in table2:
        rec_pol = record.rec[:10]
        if rec_pol in pl_list:
            re = rec_pol[0:7]
            re1 = rec_pol[8:10]
            re2 = re + re1
            pl_app[record.rec] = (re2 +','+ record.name.replace(",","") +','+ record.street.replace(",","") + ',' + record.city.replace(",","") + ',' + record.st + ',' + record.zip)
        else:
            continue
    table2.close()
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Reports")
    
    with open(c_name, 'w') as c:
        c.write('REC,PolicyREC,Address,PremInfo,PremDesc1\n')
        for key in cl_app.keys():
            c.write("%s,%s\n"%(key,cl_app[key]))
    c.close()
    
    with open(p_name , 'w') as p:
        p.write('REC,PolicyREC,Name,Street,City,State,Zip\n')
        for key in pl_app.keys():
            p.write("%s,%s\n"%(key,pl_app[key]))
    p.close()
    input("PL and CL App Data written to CSV......")
    
    #Closing AP2 database

main()
