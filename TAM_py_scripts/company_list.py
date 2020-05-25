'''
Created on Aug 28, 2019

@author: michael
'''
import os
import dbf


def main():
    #Declaring main variables
    
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data")
    bco = []
    
    table = dbf.Db3Table("INS.DBF")
    table.open(mode=dbf.READ_WRITE)
    table.use_deleted = False
    
    #Skip deleted records
    
    print ("INS Database Size: %d" % table.__len__())
    
    for record in dbf.Process(table):
        
        if record.key == "Y":
            
        #End of if C for customer
    table.close()
    print("Number of INS.dbf records Processed %d" % (z))
#Closing INS Database and DBF
