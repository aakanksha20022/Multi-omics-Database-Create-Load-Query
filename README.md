Multiomics-Database-Create-Load-Query
---
This python program implements a bioinformatic-database load and fetch utility. It creates a database storing the complex multi-omics data from the paper: ‘Personal aging markers and ageotypes 
revealed by deep longitudinal profiling.’ The data consists of a mixture of pre-processed files produced from the measurements of the transcripts, proteins, and metabolites of a cohort of 106 
individuals in the study.

Entity-Relationship diagram was created to visualise the schema for the database:
![image](https://github.com/user-attachments/assets/fc3bc130-b2d8-417e-bcd9-cc2e9c295c43)


All the files were hard coded as all the functionalities were catered to answer a specific set of queries from the this dataset.

The following command-line arguments must be provided in order to run the script:
python3 2875662.py –createdb –loaddb –querydb (1-9) db_2875662.db

1. The –-createdb option should be used to create the database structure.
2. The –-loaddb option should be used to parse the data files and insert the relevant data into the database. 
3. The –-querydb option should be used to run one of the queries specified below on the created and loaded database.

The input files consist of:
1. Subject.csv holds information on the 106 subjects of the study.
2. HMP_transcriptome_abundance.tsv, HMP_proteome_abundance.tsv, HMP_metabolome_abundance.tsv are tab-separated files that contain the measurements of transcriptomics, proteomics and
   metabolomics (peaks) of the different samples from the subjects. Peaks (columns in HMP_metabolome_abundance.tsv) are annotated with their metabolite identities, and this information is
   stored in the HMP_metabolome_annotation.csv file linking a peak ID to its metabolite name, KEGG and HMDB ID, the chemical class and pathway where the metabolite could be found.

Queries:

1. Retrieve SubjectID and Age of subjects whose age is greater than 70.

2. Retrieve all female SubjectID who have a healthy BMI (18.5 to 24.9). Sort the results in descending order.

3. Retrieve the Visit IDs of Subject 'ZNQOVZV'. This query will be easy if the Visit ID information has been correctly parsed and stored into the database.

4. Retrieve distinct SubjectIDs who have metabolomics samples and are insulin-resistant.

5. Retrieve the unique KEGG IDs that have been annotated for the following peaks: 
a. 'nHILIC_121.0505_3.5'
b. 'nHILIC_130.0872_6.3'
c. 'nHILIC_133.0506_2.3'
d. 'nHILIC_133.0506_4.4'
This query will be easy if the annotation information has been correctly parsed and stored into the database.

6. Retrieve the minimum, maximum and average age of Subjects.

7. Retrieve the list of pathways from the annotation data, and the count of how many times each pathway has been annotated. Display only pathways that have a count of at least 10. Order the
   results by the number of annotations in descending order.

8. Retrieve the maximum abundance of the transcript 'A1BG' for subject 'ZOZOW1T' 
across all samples.

9. Retrieve the subjects’ age and BMI. If there are NULL values in the Age or BMI columns, that subject should be omitted from the results. At the same time, generate a scatter plot of age vs
    BMI using the query results from above.


