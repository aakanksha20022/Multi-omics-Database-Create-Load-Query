
#Importing the relevant modules
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import argparse

#Creating the DatabaseManager class with various methods to create and interact with the database
class DatabaseManager:
    #Initializing the DatabaseManager class with the specified SQLite database file.
    def __init__(self, db_2875662):
        self.db_2875662 = db_2875662

        #Establishing a connection to the SQlite database
        self.connection = sqlite3.connect(self.db_2875662)

        #Creating a cursor to execute SQL commands within the database
        self.cursor = self.connection.cursor()

    #Creating a method for the creation of subject and annotation tables
    #Using the execute function to create the tables
    def create_subject_annot(self):
        try:
            cursor = self.connection.cursor()

            #SubjectID is set as the primary key
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Subject (
                    SubjectID TEXT PRIMARY KEY,
                    Sex TEXT,
                    Age INTEGER,
                    BMI REAL,
                    InsulinSensitivity TEXT
                )
            ''')

            #Combination of PeakID and metabolite name is used as the primary key
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Annotation (
                    PeakID TEXT,
                    MetaboliteName TEXT,
                    KEGG TEXT,
                    ChemicalClass TEXT,
                    Pathway TEXT,
                    PRIMARY KEY (PeakID, MetaboliteName)
                )
            ''')
            #Commiting the changes to the database
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Error creating schema: {e}")
            raise SystemExit(1)
        
        
    #Creating a method to create the abundance tables for protein, transcript and metabolite samples
    def create_abundance(self, file_path, table_name):

        #Helper function to ammend column names by replacing invalid characters
        def new_column_name(column):
            return column.replace("-", "_").replace(".", "_")

        try:
            #Creating a cursor to execute SQL commands
            cursor = self.connection.cursor()

            #Reading the abundance files
            with open(file_path) as trans_file:
                header = next(trans_file)

                #Creating a list of all the headers
                headers = header.rstrip().split('\t')

                #Replacing the 'SampleID' with 'SubjectID in the headers list
                headers[headers.index('SampleID')] = 'SubjectID'

                #Creating a list with all the peakIDs/protein names/transcript names
                abundance = header.strip().split()[1:]

                #Generating unique column names
                #Utilising the new_column_name function to replace invalid characters in peakIDs/protein names/transcript names
                abundance = [new_column_name(trans_name) for trans_name in abundance]

                # Initializing a set to keep track of seen column names with potential duplicates.
                duplicates = set()

                # Iterate over the abundance column names.
                for i, name in enumerate(abundance):
                    # Checking if the name is already in the set of duplicates.
                    if name in duplicates:
                        # If it is a duplicate, appending a unique suffix to make it unique.
                        abundance[i] += f"_{i + 1}"
                    # Adding the current name to the set of duplicates.
                    duplicates.add(name)

                # Creating the table query dynamically based on the column names.
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        SubjectID TEXT,
                        VisitID TEXT(100),
                '''

                for trans_name in abundance:
                    create_table_query += f'{trans_name} REAL,'

                create_table_query += '''
                    PRIMARY KEY (SubjectID, VisitID),
                    FOREIGN KEY (SubjectID) REFERENCES Subject(SubjectID)
                );
                '''

                cursor.execute(create_table_query)

                #Returning unique peak IDs/ protein names/ transcript names with alid characters
                return abundance
        except sqlite3.Error as e:
            print(f"Error creating table: {e}")
            raise SystemExit(1)
        
        
    #Creating a method to load data into the subject table
    def insert_subject(self, subject_file):
        try:
            # Checking if a connection exists, if not, establishing a new one.
            if not self.connection:
                self.connection = sqlite3.connect(self.db_2875662)
                self.cursor = self.connection.cursor()

            #Reading the subject file
            with open(subject_file, 'r') as file:
                #skipping the header
                next(file)

                #Iterating over the subject file
                for line in file:
                    line = line.strip().split(',')

                    #Setting the NA and Unknown values to none
                    for i in range(len(line)):
                        if line[i] in ['NA', 'Unknown']:
                            line[i] = None
                    #Extracting the subject_id, sex, age, bmi, insulin_sensitivity values
                    subject_id, race, sex, age, bmi, sspg, insulin_sensitivity = line

                    # Executing the SQL query to insert data into the Subject table.
                    self.cursor.execute('''
                        INSERT OR IGNORE INTO Subject (SubjectID, Sex, Age, BMI, InsulinSensitivity)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (subject_id, sex, age, bmi, insulin_sensitivity))
            # Committing changes to the database.
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Error loading subject data: {e}")
        

    #Creating a method to insert data into the abundance tables
    def insert_abundance(self, file_path, table_name, column_names):
        try:
            #Creating a cursor to execute SQL commands
            cursor = self.connection.cursor()

            #Reading the abundance files
            with open(file_path) as trans_file:
                #Skipping the header
                #Making a list of all the headers
                header = next(trans_file)
                headers = header.rstrip().split('\t')
                headers[headers.index('SampleID')] = 'SubjectID'

                # Initializing a list to store the abundance data.
                #Iterating over each line
                abundance_data = []
                for trans_exp in trans_file:
                    data = trans_exp.rstrip().split('\t')

                    #Extracting the subjectID and visitID from the sampleID column
                    subject_id, *visit_id = data[headers.index('SubjectID')].replace('-', '_').split('_')
                    # Appending the processed data to the abundance_data list.
                    abundance_data.append([subject_id, '_'.join(visit_id)] + data[1:])

                # Iterating over the processed abundance data and insert into the specified table.
                for data_row in abundance_data:
                    # Creating placeholders for the SQL query.
                    placeholders = ', '.join(['?'] * len(data_row))
                    # Creating the SQL query for inserting data into the specified table.
                    insert_data_sql = f'''
                        INSERT INTO {table_name} ({', '.join(['SubjectID', 'VisitID'] + column_names)})
                        VALUES ({placeholders})
                    '''
                    # Executing the SQL query to insert data into the table.
                    cursor.execute(insert_data_sql, data_row)
        except sqlite3.Error as e:
            print(f"Error inserting data: {e}")
        
    #Creating a method to insert data into the Annotation table
    def insert_annotation(self, annotation_file):
        try:
            if not self.connection:
                self.connection = sqlite3.connect(self.db_2875662)
                self.cursor = self.connection.cursor()

            with open("HMP_metabolome_annotation.csv") as annot_file:
                header = next(annot_file) #skipping the header
                for annot in annot_file:
                    try:
                        annot = annot.rstrip().split(',')
                        
                        #setting the empty values to None
                        for i in range(len(annot)):
                            if annot[i] == '':
                                annot[i] = None
                        #extracting the peak_id, metabolite_name, kegg, hmdb, chem_class, pathway values
                        peak_id, metabolite_name, kegg, hmdb, chem_class, pathway = annot[0], annot[1], annot[2], annot[3], annot[4], annot[5]

                        #merging the same metabolites with different peak_ids
                        if metabolite_name.endswith(('(1)', '(2)', '(3)', '(4)', '(5)')):
                            metabolite = metabolite_name.rsplit('(')[0] 
                        else:
                            metabolite = metabolite_name

                        #differentiating the metabolites and their respective kegg ids with the same peak_id seperated with a pipe
                        #so each metabolite is inserted into a seperate row
                        #using the insert_db method to insert the data complying with each condition
                        if metabolite is not None and ('|' in metabolite):
                            metabolite_list = metabolite.split('|')
                            
                            if kegg is not None and ('|' in kegg):
                                kegg_list = kegg.split('|')
                                
                                for a, b in zip(metabolite_list, kegg_list):
                                    self.cursor.execute('''INSERT INTO Annotation (PeakID, MetaboliteName, KEGG,  ChemicalClass, Pathway)
                                        VALUES (?, ?, ?, ?, ?);
                                    ''', (peak_id, a, b, chem_class, pathway))
                                    #print(f'Inserted values: PeakID={peak_id}\tMetaboliteName={a}\tKEGG={b}\tHMDB={hmdb}\tChemicalClass={chem_class}\tPathway={pathway}')
                            #if kegg value is none        
                            else:
                            
                                for a in metabolite_list:
                                    self.cursor.execute('''
                                        INSERT INTO Annotation (PeakID, MetaboliteName, KEGG, ChemicalClass, Pathway)
                                        VALUES (?, ?, ?, ?, ?);
                                    ''', (peak_id, a, kegg, chem_class, pathway))
                                    #print(f'Inserted values: PeakID={peak_id}\tMetaboliteName={a}\tKEGG={kegg}\tHMDB={hmdb}\tChemicalClass={chem_class}\tPathway={pathway}')
                        #if no pipe in the metabolite name           
                        else:
                            # Handle the case where base_metabolite_name is None or does not contain '|'
                            self.cursor.execute('''
                                INSERT INTO Annotation (PeakID, MetaboliteName, KEGG, ChemicalClass, Pathway)
                                VALUES (?, ?, ?, ?, ?);
                            ''', (peak_id, metabolite, kegg, chem_class, pathway))
                    
                        #print(f'Inserted values: PeakID={peak_id}\tMetaboliteName={metabolite}\tKEGG={kegg}\tHMDB={hmdb}\tChemicalClass={chem_class}\tPathway={pathway}')
                    except Exception as inner_error:
                       print(f"Error processing a line in the annotation file: {inner_error}") 
            self.connection.commit()
        except Exception as outer_error:
            print(f"An error occurred while loading annotation data: {outer_error}")
        
    #Method for querying the database with a givin SQL statement               
    def query_db(self, sql):
        try:
        #connect to database
            connection= sqlite3.connect(self.db_2875662)
            #creating a cursor for SQL commands
            cur= connection.cursor()
            #Execute the provided query
            cur.execute(sql)
            #Fetching all rows from the query result
            rows= cur.fetchall()
            return rows
        except Exception as query_error:
            print(f"An error occurred while querying the database: {query_error}")

    #Method for printing the result of a database query
    def query_printer(self, result):
        for row in result:
            print('\t'.join(map(str, row)))
    
    def close_connection(self):
        try:
            self.connection.close()
        except sqlite3.Error as e:
            print(f"Error closing connection: {e}")
    
    # Method for running a query on the database and printing the result
    def run_query_on_database(self, query_num):
    
        # parameterized statements for queries 1-9
        # Retrieving the SubjectID and Age of subjects whose age is greater than 70
        if query_num == 1:
            sql1 = '''
            SELECT SubjectID, Age
            FROM Subject
            WHERE Age > 70;
        '''
            query1 = self.query_db(sql1)  # using the query_db method on the db object
            self.query_printer(query1)  # using the printer method on the db object

        # Retrieving all female SubjectID who have a healthy BMI (18.5 to 24.9) and sorting the results by bmi in descending order
        if query_num == 2:
            sql2 = '''
                        SELECT SubjectID
                        FROM Subject
                        WHERE Sex = 'F' AND BMI BETWEEN 18.5 and 24.9
                        ORDER BY BMI DESC
            '''
            query2 = self.query_db(sql2)
            self.query_printer(query2)

        # Retrieving the Visit IDs of Subject 'ZNQOVZV'
        # using the union function to select all the visit_ids across all abundance tables
        # using the distinct operator so only unique visit_ids are extracted among the three tables
        if query_num == 3:

            sql3 = '''
            SELECT DISTINCT VisitID
            FROM TranscriptSamples
            WHERE SubjectID = 'ZNQOVZV'


            UNION

            SELECT DISTINCT VisitID
            FROM MetaboliteSamples
            WHERE SubjectID = 'ZNQOVZV'


            UNION

            SELECT DISTINCT VisitID
            FROM ProteinSamples
            WHERE SubjectID = 'ZNQOVZV'
            '''
            query3 = self.query_db(sql3)
            self.query_printer(query3)

        # Retrieving the distinct SubjectIDs who have metabolomics samples and are insulin-resistant

        if query_num == 4:
            sql4 = '''
                SELECT DISTINCT MetaboliteSamples.SubjectID
                FROM MetaboliteSamples,
                    Subject
                WHERE InsulinSensitivity = 'IR'
                AND MetaboliteSamples.SubjectID = Subject.SubjectID
                '''

            query4 = self.query_db(sql4)
            self.query_printer(query4)

        # Retrieving the unique KEGG IDs that have been annotated for the following peaks:
        # 'nHILIC_121.0505_3.5', 'nHILIC_130.0872_6.3', 'nHILIC_133.0506_2.3', 'nHILIC_133.0506_4.4 '

        if query_num == 5:
            sql5 = ''' SELECT DISTINCT KEGG
                FROM Annotation
                WHERE PeakID in ('nHILIC_121.0505_3.5', 'nHILIC_130.0872_6.3', 'nHILIC_133.0506_2.3', 'nHILIC_133.0506_4.4')
                '''

            query5 = self.query_db(sql5)
            self.query_printer(query5)

        # Retrieving the minimum, maximum and average age of Subjects
        if query_num == 6:
            sql6 = '''SELECT MIN(Age) AS min_age,
                    MAX(Age) AS max_age,
                    AVG(Age) AS avg_age
                    FROM Subject'''

            query6 = self.query_db(sql6)
            self.query_printer(query6)

        # retrieving each pathway and the number of times it has been annotated
        # only displaying the pathways with counts greater than 10
        # ordering the results by the count in descending order
        if query_num == 7:
            sql7 = '''
                    SELECT Pathway, COUNT(*) AS count
                    FROM Annotation
                    WHERE Pathway IS NOT NULL
                    GROUP BY Pathway
                    HAVING count > 10
                    ORDER BY count DESC
                '''

            query7 = self.query_db(sql7)
            self.query_printer(query7)

        # Retrieving the maximum abundance of the transcript 'A1BG' for subject 'ZOZOW1T' across all samples

        if query_num == 8:
            sql8 = '''SELECT MAX(A1BG) as max_abundance
                FROM TranscriptSamples
                WHERE SubjectID = 'ZOZOW1T'
                    
                '''

            query8 = self.query_db(sql8)
            self.query_printer(query8)

        # retrieving the age and bmi of of the subjects
        # omitting the null values
        if query_num == 9:

            sql9 = '''
                SELECT SubjectID, Age, BMI
                FROM Subject
                WHERE Age IS NOT NULL AND BMI IS NOT NULL'''

            query9 = self.query_db(sql9)
            self.query_printer(query9)

            # Create a DataFrame from the query result
            df = pd.DataFrame(query9, columns=["SubjectID", "Age", "BMI"])

            # Generate a scatter plot
            plt.scatter(df['Age'], df['BMI'])
            plt.xlabel('Age')
            plt.ylabel('BMI')
            plt.title('Scatter Plot of Age vs BMI')
            plt.grid(True)

            # Save the scatter plot as a PNG file
            plt.savefig('age_bmi_scatterplot.png')
            plt.show()

# Define the main function for the Database Manager script.
def main():
    # Create an ArgumentParser object to handle command line arguments.
    parser = argparse.ArgumentParser(description="Database Manager Script")

    # Specify command line arguments.
    parser.add_argument("db_2875662", help="Path to the SQLite database file")
    parser.add_argument("--createdb", action="store_true", help="Create the database structure")
    parser.add_argument("--loaddb", action="store_true", help="Parse data files and insert relevant data into the database")
    parser.add_argument("--querydb", type=int, choices=range(1, 10), help="Run one of the specified queries (1 to 9)")

    # Parse the command line arguments.
    args = parser.parse_args()

    # Create an instance of the DatabaseManager with the specified SQLite database file.
    db_manager = DatabaseManager(args.db_2875662)

    # Check if the --createdb flag is provided, and create the database structure if true.
    if args.createdb:
        db_manager.create_subject_annot()

    # Check if the --loaddb flag is provided, and load data into the database if true.
    if args.loaddb:
        # Load data into the Subject table from "Subject.csv".
        db_manager.insert_subject("Subject.csv")

        # Create abundance tables and insert data for proteome, metabolome, and transcriptome.
        proteome_columns = db_manager.create_abundance('HMP_proteome_abundance.tsv', 'ProteinSamples')
        metabolome_columns = db_manager.create_abundance('HMP_metabolome_abundance.tsv', 'MetaboliteSamples')
        transcriptome_columns = db_manager.create_abundance('HMP_transcriptome_abundance.tsv', 'TranscriptSamples')
        db_manager.insert_abundance('HMP_proteome_abundance.tsv', 'ProteinSamples', proteome_columns)
        db_manager.insert_abundance('HMP_metabolome_abundance.tsv', 'MetaboliteSamples', metabolome_columns)
        db_manager.insert_abundance('HMP_transcriptome_abundance.tsv', 'TranscriptSamples', transcriptome_columns)

        # Load annotation data into the database from "HMP_metabolome_annotation.csv".
        db_manager.insert_annotation("HMP_metabolome_annotation.csv")

    # Check if the --querydb flag is provided, and run the specified query if true.
    if args.querydb is not None:
        db_manager.run_query_on_database(args.querydb)
    
    #Implementing the close_connection method
    db_manager.close_connection()

# Entry point: Execute the main function if the script is run as the main module.
if __name__ == "__main__":
    main()
  

