'''
Created on Oct 15, 2019

@author: Michael Nelson
'''
import os
import dbf
import csv

def main():
    #Declaring main variables CSR/Drive
    polNum_csv = input("Input CSV NAME: ")
    pol_list = {}
    
    co_csv(polNum_csv,pol_list)
    pol_match(pol_list)
    
##reading csv of Policy Numbers
def co_csv(polNum_csv,pol_list):
    os.chdir("D:\\TAM\\Data")
    with open(polNum_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
                continue
            else:
                pol_list[row[0]] = ""
                line_count += 1
        print (line_count)
        input("Press Enter to exit......")

def pol_match(pol_list):
    #Reading DBF file / Changing to DBF Directory
    os.chdir("D:\\TAM\\Data")
    
    table = dbf.Db3Table("POLICY.DBF")
    table.open(mode=dbf.READ_WRITE)
    #Skip deleted records
    table.use_deleted = False
    
    f_name = input("Please Enter File Name: ")
    z = 0
    
    for record in dbf.Process(table):
        #Variables for information matching
        #strip the Policy Number
        pol = record.pol
        pol = pol.strip()
        #Strip and remove Policy Number for Client Code
        client = record.pol_idx
        client = client[0:7]
        #Strip blank spaces on CSR and PR fields
        csr = record.csr
        pr = record.pr
        
        #check if 100% match
        if pol in pol_list:
            
            z += 1
            pol_list[pol]= {'CSR': csr.strip(),'PR': pr.strip(), 'Code': client.strip()}
        else:
            num = len(pol)
            #75% and 50% match and removing decimal points
            numb = (num * .75)
            nums = (num * .5)
            
            #DB3 variables for matching 75%
            n = pol[0:(int(numb))]
            #DB3 variables for matching 50%
            p = pol[0:(int(nums))]
            
            for x,i in pol_list.items():
                #CSV variables for matching 75%
                m = x
                m = m[0:(int(numb))]
                #CSV variables for matching 50%
                o = x
                o = o[0:(int(nums))]
                
                #75 percent match check
                if m is n:
                    print(".....75.......")
                    print (numb)
                    print (m)
                    print(n)
                    print (pol)
                    input(".......Pause.........")
                    #check if Value is added
                    if i is client:
                        continue
                    elif i is "":
                        pol_list[x] = client
                    else:
                        va = (i + "," + client)
                        pol_list[x] = va
                elif o is p:
                    print(".....50.......")
                    print (nums)
                    print (p)
                    print(o)
                    input(".......Pause.........")
                    #check if Value is added
                    if i is client:
                        continue
                    elif i is "":
                        pol_list[x] = client
                    else:
                        va1 = (i + "," + client)
                        pol_list[x] = va1
                else:
                    continue
        #End of Pol match for customer
    table.close()
    
    os.chdir("D:\\TAM\\Reports")
    
    with open(f_name , 'w') as f:
        f.write('Pol_Number,CSR,PR,TAM Client Code\n')
        for p,p_info in pol_list.items():
            f.write("%s,"%(p))
            print (pol_list[p])
            for key in p_info:
                f.write(p_info[key] + ",")
            f.write("\n")
        f.close()
        
    print("Number of Policy records Processed %d" % (z))
#Closing INS Database and DBF
    
main()
