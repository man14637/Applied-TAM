'''
Created on Sep 29, 2019

@author: michael
'''
import os
import csv

def main():
    os.chdir("C:\\Users\\michael\\Documents\\TAM\\Data\\reassign")
    
    client_csv = input("Input CSV NAME: ")
    cl_code = {}
   
    with open(client_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            cl_code.append(row[0])
            line_count += 1
        print (line_count)
    

main()