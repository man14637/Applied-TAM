#!/usr/bin/python3

import os
import dbf

def main():
	#Declaring main variables CSR/Drive
	csrOne = input("Please enter the CSR Code you would like to reassign:")
	csrTwo = input("Please enter the CSR code that you are reassigning too:")
	
	ins(csrOne,csrTwo)
	policy(csrOne,csrTwo)

def ins(csrOne,csrTwo) :
	#Reading DBF file / Changing to DBF Directory
	
	os.chdir("G:\\TAM")
	
	table = dbf.Db3Table("INS.DBF")
	table.open(mode=dbf.READ_WRITE)
	table.use_deleted = False
	
	z = 0
	tamC = ''
	aa = ''
	st = ''
	str.encode('latin-1')
	csrOne.encode('latin-1')
	csrTwo.encode('latin-1')
	
	#Skip deleted records
	
	print ("INS Database Size: %d" % table.__len__())
	
	for record in dbf.Process(table):
		st = record.c
		aa = st[:2]
		
		if aa == csrOne.upper():
			z = z + 1
			num = int(len(st))
			aa = csrTwo.upper()
			ab = st[2:num]
			tamC = aa + ab
			tamC.encode('latin-1')
			record.c = tamC
			#End of if C for customer
	table.close()
	print("Number of INS.dbf records Processed %d" % (z))
	input("Press Enter to Continue to Policy Database......")
#Closing INS Database and DBF

def policy(csrOne,csrTwo):
	#Reading DBF file / Changing to DBF Directory
	
	os.chdir("G:\\TAM")
	
	table = dbf.Db3Table("POLICY.DBF")
	table.use_deleted = False
	table.open(mode=dbf.READ_WRITE)
	print ("Database Size: %d" % table.__len__())
	
	name_index = table.create_index(lambda rec: rec.csr)
	
	
	z = 0
	csrOne.encode('latin-1')
	csrTwo.encode('latin-1')
	
	#Skip deleted records
	
	for record in dbf.Process(name_index):
		
		if record.csr == csrOne.upper():
			z = z + 1
			record.csr = csrTwo.upper()
		#End of if C for customer
	table.close()
	print("Number of Policy.dbf records Processed %d" % (z))
	input("Press Enter to exit......")
#Closing INS Database and DBF

main()
