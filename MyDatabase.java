/*** Memory Efficient Pharmaceutical Database  *******/
/*** Programming project 2: Files and Indexing *******/
/*** CS6360-Database Design                    *******/
// Author : Malini Kottarappatt Bhaskaran 
// NETID  : mxk152030

package com.pkg;

//Importing the opencsv files from the jar file added to library for handling CSV files
import com.opencsv.CSVReader;
//Importing the Java classes used in the program
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class MyDatabase {

	/** Variable Declaration ***/

	String directory; // To store current directory
	File path_csv; // To get the CSV file path
	File path_data; // To get the binary data file storage location
	File[] idx_files; // Array of index file

	/**** Constants for Bit manipulation **********/
	final static int delete_flag = 128;
	final static byte double_blind_mask = 8; // binary 0000 1000
	final static byte controlled_study_mask = 4; // binary 0000 0100
	final static byte govt_funded_mask = 2; // binary 0000 0010
	final static byte fda_approved_mask = 1; // binary 0000 0001

	String fileName;

	char flagRecFound = 'X';

	int userCh = 0;

	/***** Index types *********/
	TreeMap<Integer, Long> id_idx;
	TreeMap<String, ArrayList<Long>> company_idx, drug_id_idx;
	TreeMap<Short, ArrayList<Long>> trials_idx, patients_idx, dosage_mg_idx;
	TreeMap<Float, ArrayList<Long>> reading_idx;
	TreeMap<Boolean, ArrayList<Long>> deletion_idx, double_blind_idx, controlled_study_idx, govt_funded_idx,
			fda_approved_idx;

	/*** Constructor ****/
	public MyDatabase() {
		// Get current system directory
		directory = System.getProperty("user.dir");
		// Make the File path.Ex: Current_dir/PHARMA_TRIALS_1000B.csv
		System.out.println("Enter the CSV File name without extension");
		fileName = getChoice();
		path_csv = new File(directory + File.separator + fileName + ".csv");
		// path_csv = new File(directory + File.separator +
		// "PHARMA_TRIALS_1000B.csv");
		// Make the path_data for the binary data file
		path_data = new File(directory + File.separator + fileName + ".db");
		// path_data = new File(directory + File.separator +
		// "PHARMA_TRIALS_1000B.db");

		id_idx = new TreeMap<>();
		company_idx = new TreeMap<>();
		drug_id_idx = new TreeMap<>();
		trials_idx = new TreeMap<>();
		patients_idx = new TreeMap<>();
		dosage_mg_idx = new TreeMap<>();
		reading_idx = new TreeMap<>();
		deletion_idx = new TreeMap<>();
		double_blind_idx = new TreeMap<>();
		controlled_study_idx = new TreeMap<>();
		govt_funded_idx = new TreeMap<>();
		fda_approved_idx = new TreeMap<>();

		/** Creating file path for each index files ***/
		idx_files = new File[12];
		idx_files[0] = new File(directory + File.separator + "id.ndx");
		idx_files[1] = new File(directory + File.separator + "company.ndx");
		idx_files[2] = new File(directory + File.separator + "drug_id.ndx");
		idx_files[3] = new File(directory + File.separator + "trials.ndx");
		idx_files[4] = new File(directory + File.separator + "patients.ndx");
		idx_files[5] = new File(directory + File.separator + "dosage_mg.ndx");
		idx_files[6] = new File(directory + File.separator + "reading.ndx");
		idx_files[7] = new File(directory + File.separator + "deletion.ndx");
		idx_files[8] = new File(directory + File.separator + "double_blind.ndx");
		idx_files[9] = new File(directory + File.separator + "controlled_study.ndx");
		idx_files[10] = new File(directory + File.separator + "govt_funded.ndx");
		idx_files[11] = new File(directory + File.separator + "fda_approved.ndx");

	}

	/***** Display Program Menu *****/
	public void displayMenu() throws Exception {
		// int userCh = 0;
		do {
			if (userCh == 0) {
				System.out.println("*************Welcome!!!!*************");
				System.out.println("**Memory-Efficient Pharma Database**");
				System.out.println("\t Operations allowed");
				System.out.println("\t 1. Import CSV file ");
				System.out.println("\t 2. Query ");
				System.out.println("\t 3. Insert ");
				System.out.println("\t 4. Delete ");
				System.out.println("\t Enter your Choice: ");
			} else {
				System.out.println("\t Invalid Choice!.. Enter value between 1-4");
			}

			userCh = Integer.parseInt(getChoice());
		} while (!(userCh >= 1 && userCh <= 4));
		getSubMenu(userCh);
	}

	/*** To get the user choice ****/
	private String getChoice() {
		// Getting User choice for the menu
		String userCh = "0";
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			userCh = br.readLine();
		} catch (Exception e) {
			System.err.println("Runtime Exception" + e);
			System.err.println("Coming out of Program");
		}
		return userCh;
	}

	/**** Get Sub menu displayed for the Main operations ****/

	private void getSubMenu(int userCh) {
		try {

			if (userCh == 1) // Import CSV to binary file
			{
				import_db();// Calls Improt function
			}
			// If User choice is Query /Insert /Delete
			if (userCh >= 2 && userCh <= 4) {
				// Check if the binary file exist
				if (!path_data.isFile()) {
					System.out.println("Database binary missing.... \nBuilding a new Database");
					System.out.println("Press Enter to continue");
					/*** Again get the userchoice ***/
					displayMenu();
				}
				switch (userCh) {
				case 1:
					// Build binary file and create index files
					import_db();
					break;
				case 2:
					// Get the required data
					queryRec();
					break;
				case 3:
					// Insert New Record
					insertRec();
					break;
				case 4:
					// Delete Record..
					deleteRec();
					break;
				default:
					System.out.println("Invalid Option");
					break;
				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void import_db() {
		/*** Building binary database file from CSV file ***/
		// Check if CSV file exist in the file path
		if (!path_csv.isFile()) {
			// If not then give the error message
			System.err.println("CSV file missing at " + path_csv.toString());
			System.exit(1);
		}
		try {
			// csvRead object
			CSVReader csvRead = new CSVReader(new FileReader(path_csv), ',', '"', 1);
			// Output file object
			RandomAccessFile binOut = new RandomAccessFile(path_data, "rw");
			// Input read line
			String[] inputLine;

			while ((inputLine = csvRead.readNext()) != null) {
				if (inputLine != null) {
					long filepointer = binOut.getFilePointer();

					/*********** Column : ID **********/
					// Id - Datatype -Integer
					// Writing Id value to bin file and putting to index file
					int id = Integer.parseInt(inputLine[0]);
					// writeInt function writes Int value as four bytes to the
					// binary file
					binOut.writeInt(id);
					// Add index to the ID index file
					id_idx.put(id, filepointer);

					/********** Column: Company *******/
					// Company-Data Type- varchar
					// Get the company value from index 1 of array
					String company = inputLine[1];
					// Get the size of the string
					// int size = company.length();
					Byte size = (byte) company.length();
					// Write the size of the string to the binary file, then
					// write the string (Because Company is having Varchar type)
					// binOut.writeInt(size);
					binOut.writeByte(size);
					// Write the string
					// binOut.writeChars(company);
					// Byte company1 = Byte.parseByte(company);
					binOut.writeBytes(company);
					company_idx.put(company, getArrayList(company, filepointer, company_idx));

					/********** Column: Drug ID *******/
					// DrugID -Data Type-Char6
					String drugId = inputLine[2];
					// Write drugID directly as it is of size characters(6)
					// Byte drugID = Byte.parseByte(drugId);
					binOut.writeBytes(drugId);
					// binOut.writeChars(drugId);
					drug_id_idx.put(drugId, getArrayList(drugId, filepointer, drug_id_idx));

					/********** Column: Trials *******/
					// Trials -Datatype Short int
					short trials = (short) Integer.parseInt(inputLine[3]);
					binOut.writeShort(trials);
					// Calls getArrayList short version
					trials_idx.put(trials, getArrayList(trials, filepointer, trials_idx));

					/********** Column: patients *******/
					// Patients-Data Type - Short Int
					// Parses the string value as decimal short integer.
					short patients = Short.parseShort(inputLine[4]);
					binOut.writeShort(patients);
					// Calls getArrayList short version
					patients_idx.put(patients, getArrayList(patients, filepointer, patients_idx));

					/********** Column: Dosage_mg *******/
					// Dosage_mg -Data type- Short Int
					// Parses the string value as decimal short integer.
					short dosage_mg = Short.parseShort(inputLine[5]);
					binOut.writeShort(dosage_mg);
					// Calls getArrayList short version
					dosage_mg_idx.put(dosage_mg, getArrayList(dosage_mg, filepointer, dosage_mg_idx));

					/********** Column: Reading *******/
					// Reading - Data Type- Float
					float reading = Float.parseFloat(inputLine[6]);
					binOut.writeFloat(reading);
					reading_idx.put(reading, getArrayList(reading, filepointer, reading_idx));

					// ********1 BYTE for all boolean Values*******/
					String bool = "0000";
					/**** Column: Del Indicator ********/
					// Assigns false in the initials case as all records are not
					// in deleted stage
					boolean delInd = false;
					// write false to the binary file as one byte value.
					// binOut.writeBoolean(delInd);
					deletion_idx.put(delInd, getArrayList(delInd, filepointer, deletion_idx));

					/**** Column: Unused ********/
					// Assigns false in the initials case as all records are not
					// in deleted stage
					boolean unUsed1 = false;
					// write false to the binary file as one byte value.
					// binOut.writeBoolean(unUsed1);

					/**** Column: Unused ********/
					// Assigns false in the initials case as all records are not
					// in deleted stage
					boolean unUsed2 = false;
					// write false to the binary file as one byte value.
					// binOut.writeBoolean(unUsed2);

					/**** Column: Unused ********/
					// Assigns false in the initials case as all records are not
					// in deleted stage
					boolean unUsed3 = false;
					// write false to the binary file as one byte value.
					// binOut.writeBoolean(unUsed3);

					/**** Column: Double Blind ********/
					// Assigns true if the value is true, else assigns false
					boolean double_blind = inputLine[7].equalsIgnoreCase("true");
					// write true/false to the binary file as one byte value.
					if (inputLine[7].equalsIgnoreCase("true")) {
						bool = bool + 1;
					} else {
						bool = bool + 0;
					}
					// binOut.writeBoolean(double_blind);
					double_blind_idx.put(double_blind, getArrayList(double_blind, filepointer, double_blind_idx));

					/**** Column: controlled_study ********/
					// Assigns true if the value is true, else assigns false
					boolean controlled_study = inputLine[8].equalsIgnoreCase("true");
					// write true/false to the binary file as one byte value.
					// binOut.writeBoolean(controlled_study);
					if (inputLine[8].equalsIgnoreCase("true")) {
						bool = bool + 1;
					} else {
						bool = bool + 0;
					}
					controlled_study_idx.put(controlled_study,
							getArrayList(controlled_study, filepointer, controlled_study_idx));

					/**** Column: govt_funded ********/
					// Assigns true if the value is true, else assigns false
					boolean govt_funded = inputLine[9].equalsIgnoreCase("true");
					// write true/false to the binary file as one byte value.
					// binOut.writeBoolean(govt_funded);
					if (inputLine[9].equalsIgnoreCase("true")) {
						bool = bool + 1;
					} else {
						bool = bool + 0;
					}
					govt_funded_idx.put(govt_funded, getArrayList(govt_funded, filepointer, govt_funded_idx));

					/**** Column: fda_approved ********/
					// Assigns true if the value is true, else assigns false
					boolean fda_approved = inputLine[10].equalsIgnoreCase("true");
					// write true/false to the binary file as one byte value.
					// binOut.writeBoolean(fda_approved);
					if (inputLine[10].equalsIgnoreCase("true")) {
						bool = bool + 1;
					} else {
						bool = bool + 0;
					}
					fda_approved_idx.put(fda_approved, getArrayList(fda_approved, filepointer, fda_approved_idx));
					// Writing the Boolean value set to the file
					byte boolValues = (byte) Integer.parseInt(bool, 2);
					binOut.writeByte(boolValues);

				}
			}
			// Replacing contents in the index files
			writeIndices(id_idx, idx_files[0]);
			writeIndices(company_idx, idx_files[1]);
			writeIndices(drug_id_idx, idx_files[2]);
			writeIndices(trials_idx, idx_files[3]);
			writeIndices(patients_idx, idx_files[4]);
			writeIndices(dosage_mg_idx, idx_files[5]);
			writeIndices(reading_idx, idx_files[6]);
			writeIndices(deletion_idx, idx_files[7]);
			writeIndices(double_blind_idx, idx_files[8]);
			writeIndices(controlled_study_idx, idx_files[9]);
			writeIndices(govt_funded_idx, idx_files[10]);
			writeIndices(fda_approved_idx, idx_files[11]);
			System.out.println("Imported Successfully and Index files have been created");
			// Closing the csvRead and binary out files
			csvRead.close();
			binOut.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}

	// Writing to the index files
	private void writeIndices(TreeMap map, File file) {
		try {
			// Opens file in append mode so that Insert Record works fine
			FileWriter f = new FileWriter(file);

			for (Object key : map.keySet()) {
				String content = key.toString() + "=" + map.get(key).toString().replace("[", "")
						+ System.getProperty("line.separator");
				f.write(content.replace("]", "").replace(" ", ""));

			}
			f.close();
		} catch (Exception e) {
			System.err.println("Write failed:" + e);
		}

	}

	private void queryRec() {
		System.out.println("Making SELECT QUERY in the following format");
		System.out.println("SELECT * FROM " + fileName + " WHERE(FIELD)(OPERATION)(VALUE)");
		System.out.println("Choose FIELD and OPERATION From Menu and Give Value to the prompt");
		/** Variables for the select operation **/
		int column;
		int operation;
		String value;
		String option;
		do {
			System.out.println("Based on which field you want to select the record ?");
			column = getColumns(); // Display and Get for field values
			String col = "";
			if (column == 0)
				col = "Id";
			else if (column == 1)
				col = "Company";
			else if (column == 2)
				col = "Drug_Id";
			else if (column == 3)
				col = "Trials";
			else if (column == 4)
				col = "Patients";
			else if (column == 5)
				col = "Dosage_mg";
			else if (column == 6)
				col = "Reading";
			else if (column == 7)
				col = "Deleted";
			else if (column == 8)
				col = "Double_blind";
			else if (column == 9)
				col = "Controlled_study";
			else if (column == 10)
				col = "Govt_funded";
			else if (column == 11)
				col = "FDA_approved";

			switch (column) {
			/*** If Numeric field then get the operation to be used ***/
			case 0:
			case 3:
			case 4:
			case 5:
			case 6:
				// Get the Operation value to be used in SELECT query
				// (=,<,>,<=,>=)
				operation = getOperation();
				break;
			default:
				System.out.println("Select Which operation you want to perform");
				System.out.println("1. Equal ==" + "\n2. NOT =");
				operation = Integer.parseInt(getChoice());
				if (operation == 2) {
					// Setting the operation number as 6 for the code purpose
					operation = 6;
				}
				break;
			}
			String op = "";
			switch (operation) {
			case 1:
				op = "=";
				break;
			case 2:
				op = ">";
				break;
			case 3:
				op = "<";
				break;
			case 4:
				op = ">=";
				break;
			case 5:
				op = "<=";
				break;
			case 6:
				op = "!=";
				break;
			}
			System.out.println("Enter the value to filter: ");
			value = getChoice();

			System.out.println("Performing SELECT query");
			System.out.println("SELECT * FROM " + fileName + " WHERE " + col + " " + op + " " + value);
			try {
				getRecords(column, operation, value);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Do you wish to continue? [Y/N]: ");
			option = getChoice();
		} while (option.equalsIgnoreCase("Y"));
	}

	private void insertRec() throws Exception {
		System.out.println("Insert");
		RandomAccessFile insOut = new RandomAccessFile(path_data, "rw");
		Long lenFile = insOut.length();
		// Take offset to the end of the file
		insOut.seek(lenFile);
		// Get the new File pointer value
		long filePtr = insOut.getFilePointer();
		// Getting insert values from user
		System.out.println("Insert Statement!. Give values separated by Comma. Do not put Ending Parentheses");
		System.out.println("INSERT INTO " + fileName + " VALUES (");
		String insLine = getChoice();
		System.out.println(" )");
		String[] inputLine = insLine.split(",");
		// Populate the TreeMaps of each index file before performing new
		// insertion

		TreeMap<Integer, Long> idIdx = new TreeMap<>();
		TreeMap<String, ArrayList<Long>> compIdx = new TreeMap<>();
		TreeMap<String, ArrayList<Long>> drugIdx = new TreeMap<>();
		TreeMap<Short, ArrayList<Long>> trialsIdx = new TreeMap<>();
		TreeMap<Short, ArrayList<Long>> patientIdx = new TreeMap<>();
		TreeMap<Short, ArrayList<Long>> dosageIdx = new TreeMap<>();
		TreeMap<Float, ArrayList<Long>> readingIdx = new TreeMap<>();
		TreeMap<Boolean, ArrayList<Long>> delIdx = new TreeMap<>();
		TreeMap<Boolean, ArrayList<Long>> doubleIdx = new TreeMap<>();
		TreeMap<Boolean, ArrayList<Long>> cntrlIdx = new TreeMap<>();
		TreeMap<Boolean, ArrayList<Long>> govIdx = new TreeMap<>();
		TreeMap<Boolean, ArrayList<Long>> fdaIdx = new TreeMap<>();

		for (int i = 0; i <= 11; i++) {
			// TreeMap<Object, ArrayList<Long>> recordIdx = new TreeMap<>();
			TreeMap<String, ArrayList<Long>> recordIdx = new TreeMap<>();

			recordIdx.clear();
			// Check if the Index file is present
			if (!idx_files[i].isFile()) {
				System.err.println("Index File Missing");
				System.exit(1);
			}

			if (i == 0) {
				Long rec = null;
				Properties prp = new Properties();
				// Loads the properties of the Idx file Input stream
				prp.load(new FileInputStream(idx_files[i]));
				for (Object keys : prp.keySet()) {
					// Get the key values in Index file separated by comma in to
					// the
					// string array
					String key = (String) keys;
					int keyVal = Integer.parseInt(key);
					String val = prp.get(keys).toString();
					if (!val.isEmpty()) {
						rec = Long.parseLong(val);
					}

					idIdx.put(keyVal, rec);
				}
			} else {
				Properties prp = new Properties();
				// Loads the properties of the Idx file Input stream
				prp.load(new FileInputStream(idx_files[i]));
				for (Object keys : prp.keySet()) {
					// Get the key values in Index file separated by comma in to
					// the
					// string array
					String[] values = prp.get(keys).toString().split(",");
					ArrayList<Long> record = new ArrayList<Long>();
					String key = (String) keys;
					for (String val : values) {
						if (!val.isEmpty()) {
							record.add(Long.parseLong(val));
						}
					}
					// Add the key and values to the Hash
					// recordIdx.put((String) keys, record);

					if (i == 1) {
						compIdx.put((String) keys, record);
					} else if (i == 2) {
						drugIdx.put((String) keys, record);
					} else if (i == 3) {
						Short keyVal = Short.parseShort(key);
						trialsIdx.put(keyVal, record);
					} else if (i == 4) {
						Short keyVal = Short.parseShort(key);
						patientIdx.put(keyVal, record);
					} else if (i == 5) {
						Short keyVal = Short.parseShort(key);
						dosageIdx.put(keyVal, record);
					} else if (i == 6) {
						Float keyVal = Float.parseFloat(key);
						readingIdx.put(keyVal, record);
					} else if (i == 7) {
						Boolean keyVal = Boolean.parseBoolean(key);
						delIdx.put(keyVal, record);
					} else if (i == 8) {
						Boolean keyVal = Boolean.parseBoolean(key);
						doubleIdx.put(keyVal, record);
					} else if (i == 9) {
						Boolean keyVal = Boolean.parseBoolean(key);
						cntrlIdx.put(keyVal, record);
					} else if (i == 10) {
						Boolean keyVal = Boolean.parseBoolean(key);
						govIdx.put(keyVal, record);
					} else if (i == 11) {
						Boolean keyVal = Boolean.parseBoolean(key);
						fdaIdx.put(keyVal, record);
					}
					/*
					 * if (i == 0) { idIdx = recordIdx; } else if (i == 1) {
					 * compIdx = recordIdx; } else if (i == 2) { drugIdx =
					 * recordIdx; } else if (i == 3) { trialsIdx = recordIdx; }
					 * else if (i == 4) { patientIdx = recordIdx; } else if (i
					 * == 5) { dosageIdx = recordIdx; } else if (i == 6) {
					 * readingIdx = recordIdx; } else if (i == 7) { delIdx =
					 * recordIdx; } else if (i == 8) { doubleIdx = recordIdx; }
					 * else if (i == 9) { cntrlIdx = recordIdx; } else if (i ==
					 * 10) { govIdx = recordIdx; } else if (i == 11) { fdaIdx =
					 * recordIdx; }
					 */
				}
			}
		}

		/*********** Column : ID **********/
		// Id - Datatype -Integer
		// Writing Id value to bin file and putting to index file
		int id = Integer.parseInt(inputLine[0]);
		// writeInt function writes Int value as four bytes to the
		// binary file
		insOut.writeInt(id);
		// Add index to the ID index file
		if (idIdx.containsKey(id)) {
			System.out.println("Key Already Exist!. Please consider another value");
			System.exit(1);
		}
		// idIdx.put(id, ArrayList<Long> filePtr);

		// idIdx.put(inputLine[0], getArrayList(id, filePtr, idIdx));

		idIdx.put(id, filePtr);
		/********** Column: Company *******/
		// Company-Data Type- varchar
		// Get the company value from index 1 of array
		String company = inputLine[1];
		// Get the size of the string
		// int size = company.length();
		Byte size = (byte) company.length();
		insOut.writeByte(size);
		// Write the size of the string to the binary file, then
		// write the string (Because Company is having Varchar type)
		// insOut.writeInt(size);
		// Write the string
		// insOut.writeChars(company);
		insOut.writeBytes(company);
		compIdx.put(company, getArrayList(company, filePtr, compIdx));

		/********** Column: Drug ID *******/
		// DrugID -Data Type-Char6
		String drugId = inputLine[2];
		// Write drugID directly as it is of size characters(6)
		// insOut.writeChars(drugId);
		insOut.writeBytes(drugId);
		// drug_id_idx.put(drugId, getArrayList(drugId, filePtr, drug_id_idx));
		drugIdx.put(drugId, getArrayList(drugId, filePtr, drugIdx));

		/********** Column: Trials *******/
		// Trials -Datatype Short int
		short trials = Short.parseShort(inputLine[3]);
		insOut.writeShort(trials);
		// Calls getArrayList short version
		trialsIdx.put(trials, getArrayList(trials, filePtr, trialsIdx));

		/********** Column: patients *******/
		// Patients-Data Type - Short Int
		// Parses the string value as decimal short integer.
		short patients = Short.parseShort(inputLine[4]);
		insOut.writeShort(patients);
		// Calls getArrayList short version
		patientIdx.put(patients, getArrayList(patients, filePtr, patientIdx));

		/********** Column: Dosage_mg *******/
		// Dosage_mg -Data type- Short Int
		// Parses the string value as decimal short integer.
		short dosage_mg = Short.parseShort(inputLine[5]);
		insOut.writeShort(dosage_mg);
		// Calls getArrayList short version
		dosageIdx.put(dosage_mg, getArrayList(dosage_mg, filePtr, dosageIdx));

		/********** Column: Reading *******/
		// Reading - Data Type- Float
		float reading = Float.parseFloat(inputLine[6]);
		insOut.writeFloat(reading);
		readingIdx.put(reading, getArrayList(reading, filePtr, readingIdx));

		// ********1 BYTE for all boolean Values*******/
		String bool = "0000";
		/**** Column: Del Indicator ********/
		// Assigns false in the initials case as all records are not
		// in deleted stage
		boolean delInd = false;
		String del = Boolean.toString(delInd);
		// write false to the binary file as one byte value.
		// insOut.writeBoolean(delInd);
		delIdx.put(delInd, getArrayList(delInd, filePtr, delIdx));

		/**** Column: Unused ********/
		// Assigns false in the initials case as all records are not
		// in deleted stage
		boolean unUsed = false;
		// write false to the binary file as one byte value.
		// insOut.writeBoolean(unUsed);

		/**** Column: Unused ********/
		// Assigns false in the initials case as all records are not
		// in deleted stage
		boolean unUsed2 = false;
		// write false to the binary file as one byte value.
		// insOut.writeBoolean(unUsed2);

		/**** Column: Unused ********/
		// Assigns false in the initials case as all records are not
		// in deleted stage
		boolean unUsed3 = false;
		// write false to the binary file as one byte value.
		// insOut.writeBoolean(unUsed3);

		/**** Column: Double Blind ********/
		// Assigns true if the value is true, else assigns false
		boolean double_blind = inputLine[7].equalsIgnoreCase("true");
		// write true/false to the binary file as one byte value.
		// insOut.writeBoolean(double_blind);
		String dblind = Boolean.toString(double_blind);
		if (inputLine[7].equalsIgnoreCase("true")) {
			bool = bool + 1;
		} else {
			bool = bool + 0;
		}
		doubleIdx.put(double_blind, getArrayList(double_blind, filePtr, doubleIdx));

		/**** Column: controlled_study ********/
		// Assigns true if the value is true, else assigns false
		boolean controlled_study = inputLine[8].equalsIgnoreCase("true");
		// write true/false to the binary file as one byte value.
		// insOut.writeBoolean(controlled_study);
		String cStudy = Boolean.toString(controlled_study);
		if (inputLine[8].equalsIgnoreCase("true")) {
			bool = bool + 1;
		} else {
			bool = bool + 0;
		}
		cntrlIdx.put(controlled_study, getArrayList(controlled_study, filePtr, cntrlIdx));

		/**** Column: govt_funded ********/
		// Assigns true if the value is true, else assigns false
		boolean govt_funded = inputLine[9].equalsIgnoreCase("true");
		// write true/false to the binary file as one byte value.
		// insOut.writeBoolean(govt_funded);
		String gov = Boolean.toString(govt_funded);
		if (inputLine[9].equalsIgnoreCase("true")) {
			bool = bool + 1;
		} else {
			bool = bool + 0;
		}
		govIdx.put(govt_funded, getArrayList(govt_funded, filePtr, govIdx));

		/**** Column: fda_approved ********/
		// Assigns true if the value is true, else assigns false
		boolean fda_approved = inputLine[10].equalsIgnoreCase("true");
		// write true/false to the binary file as one byte value.
		// insOut.writeBoolean(fda_approved);
		if (inputLine[10].equalsIgnoreCase("true")) {
			bool = bool + 1;
		} else {
			bool = bool + 0;
		}
		fdaIdx.put(fda_approved, getArrayList(fda_approved, filePtr, fdaIdx));

		// Writing the Boolean value set to the file
		byte boolValues = (byte) Integer.parseInt(bool, 2);
		insOut.writeByte(boolValues);

		// Updating contents in the index files
		writeIndices(idIdx, idx_files[0]);
		writeIndices(compIdx, idx_files[1]);
		writeIndices(drugIdx, idx_files[2]);
		writeIndices(trialsIdx, idx_files[3]);
		writeIndices(patientIdx, idx_files[4]);
		writeIndices(dosageIdx, idx_files[5]);
		writeIndices(readingIdx, idx_files[6]);
		writeIndices(delIdx, idx_files[7]);
		writeIndices(doubleIdx, idx_files[8]);
		writeIndices(cntrlIdx, idx_files[9]);
		writeIndices(govIdx, idx_files[10]);
		writeIndices(fdaIdx, idx_files[11]);
		insOut.close();

		System.out.println("Inserted Successfully");

	}

	private void deleteRec() throws FileNotFoundException {

		System.out.println("Making DELETE Command in the following format");
		System.out.println("DELETE FROM " + fileName + " WHERE(FIELD)(OPERATION)(VALUE)");
		System.out.println("Choose FIELD and OPERATION From Menu and Give Value to the prompt");

		// RandomAccessFile delOut = new RandomAccessFile(path_data, "rw");
		int column;
		int operation;
		String value;
		String option;
		do {
			System.out.println("Based on which field you want to perform Delete");
			column = getColumns(); // Display and Get for field values

			String col = "";
			if (column == 0)
				col = "Id";
			else if (column == 1)
				col = "Company";
			else if (column == 2)
				col = "Drug_Id";
			else if (column == 3)
				col = "Trials";
			else if (column == 4)
				col = "Patients";
			else if (column == 5)
				col = "Dosage_mg";
			else if (column == 6)
				col = "Reading";
			else if (column == 7)
				col = "Deleted";
			else if (column == 8)
				col = "Double_blind";
			else if (column == 9)
				col = "Controlled_study";
			else if (column == 10)
				col = "Govt_funded";
			else if (column == 11)
				col = "FDA_approved";

			/** To get operations for the field ****/
			switch (column) {
			/*** If Numeric field then get the operation to be used ***/
			case 0:
			case 3:
			case 4:
			case 5:
			case 6:
				// Get the Operation value to be used in SELECT query
				// (=,<,>,<=,>=)
				operation = getOperation();
				break;
			default:
				System.out.println("Select Which operation you want to perform ");
				System.out.println("1.Equal ==" + "\n2.NOT =");
				operation = Integer.parseInt(getChoice());
				if (operation == 2) {// Overwriting the case for program
					operation = 6;
				}
				break;
			}

			String op = "";
			switch (operation) {
			case 1:
				op = "=";
				break;
			case 2:
				op = ">";
				break;
			case 3:
				op = "<";
				break;
			case 4:
				op = ">=";
				break;
			case 5:
				op = "<=";
				break;
			case 6:
				op = "!=";
				break;
			}
			System.out.println("Enter the value to delete: ");
			value = getChoice();

			System.out.println("Performing Delete Operation");
			System.out.println("DELETE FROM " + fileName + " WHERE " + col + " " + op + " " + value);

			try {
				getRecords(column, operation, value);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Do you wish to continue? [Y/N]: ");
			option = getChoice();
		} while (option.equalsIgnoreCase("Y"));

	}

	private int getColumns() {
		int field = 0;
		do {
			if (field != 0) {
				System.out.println("Invalid option selected\n Please select the correct option");
			}

			System.out.println("1. Id" + "\n2. Company" + "\n3. Drug_Id" + "\n4. Trials" + "\n5. Patients"
					+ "\n6. Dosage_mg" + "\n7. Reading" + "\n8. Deleted" + "\n9. Double_blind"
					+ "\n10. Controlled_study" + "\n11. Govt_funded" + "\n12. FDA_approved\n");
			System.out.println("Enter the field which you selected: ");
			field = Integer.parseInt(getChoice()) - 1;
		} while (field > 12 || field < 0);
		return field;
	}

	private int getOperation() {
		int operation = 0;
		do {
			if (operation != 0) {
				System.out.println("Invalid option selected\n Please select the correct option");
			}
			System.out.println("1. Equal (==)" + "\n2. Strictly Greater (>)" + "\n3. Strictly Lesser (<)"
					+ "\n4. Greater or Equal (>=)" + "\n5. Lesser or Equal (<=)" + "\n6. NOT =");
			operation = Integer.parseInt(getChoice());
		} while ((operation > 6 || operation < 1));
		return operation;
	}

	private void getRecords(int field, int operation, String filterValue) throws Exception {

		/***
		 * Get the record based on the Field, operation and value choice from
		 * user
		 ***/
		// Set the flag to X
		flagRecFound = 'X';
		TreeMap<Object, ArrayList<Long>> recordHash = new TreeMap<>();
		recordHash.clear();
		// Check if the Index file is present
		if (!idx_files[field].isFile()) {
			System.err.println("Index File Missing");
			System.exit(1);
		}
		Properties prp = new Properties();
		// Loads the properties of the Idx file Input stream
		prp.load(new FileInputStream(idx_files[field]));
		for (Object keys : prp.keySet()) {
			// Get the key values in Index file separated by comma in to the
			// string array
			String[] values = prp.get(keys).toString().split(",");
			ArrayList<Long> record = new ArrayList<Long>();
			for (String val : values) {
				if (!val.isEmpty()) {
					record.add(Long.parseLong(val));
				}
			}
			// Add the key and values to the Hash
			recordHash.put(keys, record);
		}

		// Based on the operations user wants to perform

		switch (operation) {
		// 1.Equal
		case 1:
			// If checking for company value. Then Condense the string by
			// removing the spaces as the Indexes are stored without space
			if (field == 1) {
				filterValue = filterValue.replace(" ", "");

			}
			// Check if there is a key match for the value
			if (!recordHash.containsKey(filterValue)) {
				System.out.println(filterValue + " is not present");
			} else {
				// If Query Operation Call-Display record
				if (userCh == 2) {
					displayStrFmBin(recordHash.get(filterValue));
				} else if (userCh == 4) {
					markDelete(recordHash.get(filterValue), field);
				}
			}

			break;
		// 2.Strictly Greater
		case 2:

			for (Object key : recordHash.keySet()) {
				// If the comparison is on ID field, then need to convert the
				// stored value to Int for the comparison
				if (field == 0) {
					int readId = Integer.parseInt(key.toString());
					int compID = Integer.parseInt(filterValue);
					// If the record ID is greater than the Filter value then
					// Call the display record function to get the binary record
					// info in string
					if (readId > compID) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is one among Trials/Patients/Dosage
				else if (field >= 3 && field <= 5) {
					short recordInfo = Short.parseShort(key.toString());
					short userVal = Short.parseShort(filterValue);
					if (recordInfo > userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}

					}
				}
				// If the field is Reading
				else if (field == 6) {
					float recordInfo = Float.parseFloat(key.toString());
					float userVal = Float.parseFloat(filterValue);
					if (recordInfo > userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}

					}
				} else {
					if (userCh == 2) {
						displayStrFmBin(recordHash.get(key));
					} else if (userCh == 4) {
						markDelete(recordHash.get(key), field);
					}
				}
			}
			break;
		// 3.Strictly Lesser
		case 3:
			for (Object key : recordHash.keySet()) {
				// If the comparison is on ID field, then need to convert the
				// stored value to Int for the comparison
				if (field == 0) {
					int readId = Integer.parseInt(key.toString());
					int compID = Integer.parseInt(filterValue);
					// If the record ID is greater than the Filter value then
					// Call the display record function to get the binary record
					// info in string
					if (readId < compID) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is one among Trials/Patients/Dosage_Mg
				else if (field >= 3 && field <= 5) {
					short recordInfo = Short.parseShort(key.toString());
					short userVal = Short.parseShort(filterValue);
					if (recordInfo < userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is Reading
				else if (field == 6) {
					float recordInfo = Float.parseFloat(key.toString());
					float userVal = Float.parseFloat(filterValue);
					if (recordInfo < userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				} else {
					if (userCh == 2) {
						displayStrFmBin(recordHash.get(key));
					} else if (userCh == 4) {
						markDelete(recordHash.get(key), field);
					}
				}
			}

			break;
		// 4.Greater or Equal
		case 4:

			for (Object key : recordHash.keySet()) {
				// If the comparison is on ID field, then need to convert the
				// stored value to Int for the comparison
				if (field == 0) {
					int readId = Integer.parseInt(key.toString());
					int compID = Integer.parseInt(filterValue);
					// If the record ID is greater than the Filter value then
					// Call the display record function to get the binary record
					// info in string
					if (readId >= compID) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is one among Drug_ID /Trials/Patients
				else if (field >= 3 && field <= 5) {
					short recordInfo = Short.parseShort(key.toString());
					short userVal = Short.parseShort(filterValue);
					if (recordInfo >= userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is Dosage Mg
				else if (field == 6) {
					float recordInfo = Float.parseFloat(key.toString());
					float userVal = Float.parseFloat(filterValue);
					if (recordInfo >= userVal) {
						displayStrFmBin(recordHash.get(key));
					}
				} else {
					if (userCh == 2) {
						displayStrFmBin(recordHash.get(key));
					} else if (userCh == 4) {
						markDelete(recordHash.get(key), field);
					}
				}
			}
			break;
		// 5.Lesser or Equal
		case 5:
			for (Object key : recordHash.keySet()) {
				// If the comparison is on ID field, then need to convert the
				// stored value to Int for the comparison
				if (field == 0) {
					int readId = Integer.parseInt(key.toString());
					int compID = Integer.parseInt(filterValue);
					// If the record ID is greater than the Filter value then
					// Call the display record function to get the binary record
					// info in string
					if (readId <= compID) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is one among Drug_ID /Trials/Patients
				else if (field >= 3 && field <= 5) {
					short recordInfo = Short.parseShort(key.toString());
					short userVal = Short.parseShort(filterValue);
					if (recordInfo <= userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is Dosage Mg
				else if (field == 6) {
					float recordInfo = Float.parseFloat(key.toString());
					float userVal = Float.parseFloat(filterValue);
					if (recordInfo <= userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				} else {
					if (userCh == 2) {
						displayStrFmBin(recordHash.get(key));
					} else if (userCh == 4) {
						markDelete(recordHash.get(key), field);
					}
				}
			}
			break;
		// NOT EQUAL
		case 6:
			for (Object key : recordHash.keySet()) {
				// If the comparison is on ID field, then need to convert the
				// stored value to Int for the comparison
				if (field == 0) {
					int readId = Integer.parseInt(key.toString());
					int compID = Integer.parseInt(filterValue);
					// If the record ID is greater than the Filter value then
					// Call the display record function to get the binary record
					// info in string
					if (readId != compID) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is one among Drug_ID /Trials/Patients
				else if (field >= 3 && field <= 5) {
					short recordInfo = Short.parseShort(key.toString());
					short userVal = Short.parseShort(filterValue);
					if (recordInfo != userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
				// If the field is Dosage Mg
				else if (field == 6) {
					float recordInfo = Float.parseFloat(key.toString());
					float userVal = Float.parseFloat(filterValue);
					if (recordInfo != userVal) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				} else {
					String recordInfo = key.toString();
					String userVal = filterValue;
					// Check if the strings are not equal
					if (!(recordInfo.equals(userVal))) {
						if (userCh == 2) {
							displayStrFmBin(recordHash.get(key));
						} else if (userCh == 4) {
							markDelete(recordHash.get(key), field);
						}
					}
				}
			}
			break;
		default:
			System.out.println("Invalid Operation");
			break;
		}
		// If there is no key value found and no calls to MArk Delete or
		// Displaystrmfm functions

		if (flagRecFound == 'X' && operation != 1) {
			System.out.println("No matching Record/s found");
		}
	}

	// To make the array list for the String columns
	private ArrayList<Long> getArrayList(String key, long val, Map<String, ArrayList<Long>> map) {
		ArrayList<Long> rslt = new ArrayList<>();
		// Remove spaces before checking in case of Insert Rec
		if (userCh == 3) {
			key = key.replace(" ", "");
		}
		// Check if there is a mapping for the key
		if (map.containsKey(key)) {
			// Get the value if there is a mapping and then
			rslt = map.get(key);
			// Appends the new element to the end of the list
			rslt.add(val);
		} else {
			// Appends the new element to the list since there is no existing
			// mapping
			rslt.add(val);
		}
		return rslt;
	}

	// Overriden method getArrayList for type short
	private ArrayList<Long> getArrayList(short key, long val, Map<Short, ArrayList<Long>> map) {
		ArrayList<Long> rslt = new ArrayList<>();
		// Check if there is a mapping for the key
		if (map.containsKey(key)) {
			// Get the value if there is a mapping and then
			rslt = map.get(key);
			// Appends the new element to the end of the list
			rslt.add(val);
		} else {
			rslt.add(val);
		}
		return rslt;
	}

	// Overriden method getArrayList for type float keys
	private ArrayList<Long> getArrayList(float key, long val, Map<Float, ArrayList<Long>> map) {
		ArrayList<Long> result = new ArrayList<>();
		// Check if there is a mapping for the key
		if (map.containsKey(key)) {
			// Get the value if there is a mapping and then
			result = map.get(key);
			// Appends the new element to the end of the list
			result.add(val);
		} else {
			result.add(val);
		}
		return result;
	}

	// Overriden method getArrayList for type boolean keys
	private ArrayList<Long> getArrayList(boolean key, long val, Map<Boolean, ArrayList<Long>> map) {
		ArrayList<Long> result = new ArrayList<>();
		// Check if there is a mapping for the key
		if (map.containsKey(key)) {
			// Get the value if there is a mapping and then
			result = map.get(key);
			// Appends the new element to the end of the list
			result.add(val);
		} else {
			result.add(val);
		}
		return result;
	}

	// display records form Binary file in String format
	private void displayStrFmBin(ArrayList<Long> filterValue) throws Exception {
		// Set flag to Y indicating records found
		flagRecFound = 'Y';
		if (filterValue.isEmpty()) {
			System.out.println("No matching records found");
		} else {
			// Open Data file in read mode
			RandomAccessFile ranFile = new RandomAccessFile(path_data, "r");
			// For all the record positions passed from the previous function
			for (Long fv : filterValue) {
				// Sets the offset to the position filterValue - the offset
				// position, measured in bytes from the beginning of the file,
				// at which to set the file pointer.
				ranFile.seek(fv);
				int Id = ranFile.readInt(); // Reads 4 bytes from the current
											// pointer -ID
				int len = ranFile.readByte(); // -Length of the Company field
				String st1 = new String();
				String st2 = new String();
				for (int j = 0; j < len; j++) {
					// ReadChar read maximum 2 bytes from the pointer
					// st1 = st1 + ranFile.readChar();
					st1 = st1 + (char) ranFile.readByte();
				}
				// Character 6 field.Drug ID
				for (int j = 0; j < 6; j++) {
					st2 = st2 + (char) ranFile.readByte();
				}
				// Get the record info from the file and assign it to each
				// variable for column
				short trials = ranFile.readShort(); // Trials
				short patients = ranFile.readShort(); // Patients
				short dosage_mg = ranFile.readShort(); // Dosage
				float reading = ranFile.readFloat(); // Reading
				byte boolVal = ranFile.readByte(); // REad Boolean Byte

				/*
				 * boolean deletion = ranFile.readBoolean(); // Deletion boolean
				 * unUsed1 = ranFile.readBoolean();// Unused Bit boolean unUsed2
				 * = ranFile.readBoolean();// Unused Bit boolean unUsed3 =
				 * ranFile.readBoolean();// Unused Bit boolean double_blind =
				 * ranFile.readBoolean(); // Double Blind boolean ctl_study =
				 * ranFile.readBoolean(); // Controlled Study boolean
				 * govt_funded = ranFile.readBoolean(); // Govt Funded boolean
				 * fda_approved = ranFile.readBoolean(); // FDA Approved
				 */

				String record = Integer.toString(Id) + "\t" + st1 + "\t" + st2 + "\t" + Short.toString(trials) + "\t"
						+ Short.toString(patients) + "\t" + Short.toString(dosage_mg) + "\t" + Float.toString(reading)
						+ "\t";
						/*
						 * + "\t" + Boolean.toString(double_blind) + "\t" +
						 * Boolean.toString(ctl_study) + "\t" +
						 * Boolean.toString(govt_funded) + "\t" +
						 * Boolean.toString(fda_approved);
						 */

				// If delete flag is not set.. then the record details are
				// displayed
				if ((boolVal & delete_flag) != delete_flag) {
					System.out.print(record);
					System.out.print((double_blind_mask == (byte) (boolVal & double_blind_mask)) + "\t");
					System.out.print((controlled_study_mask == (byte) (boolVal & controlled_study_mask)) + "\t");
					System.out.print((govt_funded_mask == (byte) (boolVal & govt_funded_mask)) + "\t");
					System.out.print(fda_approved_mask == (byte) (boolVal & fda_approved_mask));
					System.out.println(" ");
					;

				}
				/*
				 * else { System.out.println("The record is deleted"); }
				 */
			}
			ranFile.close();
		}
	}

	private void markDelete(ArrayList<Long> filterValue, int field) throws Exception {
		// Set flag to Y indicating records found
		flagRecFound = 'Y';
		if (filterValue.isEmpty()) {
			System.out.println("No matching records found/Records were deleted");
		} else {
			// Open Data file in read mode
			RandomAccessFile ranFile = new RandomAccessFile(path_data, "rw");
			TreeMap<Object, ArrayList<Long>> deletionIdx = new TreeMap<>();
			// For all the record positions passed from the previous function

			/**************************
			 * Update all index files after deletion
			 ****************************/
			TreeMap<Integer, Long> idIdx = new TreeMap<>();
			TreeMap<String, ArrayList<Long>> compIdx = new TreeMap<>();
			TreeMap<String, ArrayList<Long>> drugIdx = new TreeMap<>();
			TreeMap<Short, ArrayList<Long>> trialsIdx = new TreeMap<>();
			TreeMap<Short, ArrayList<Long>> patientIdx = new TreeMap<>();
			TreeMap<Short, ArrayList<Long>> dosageIdx = new TreeMap<>();
			TreeMap<Float, ArrayList<Long>> readingIdx = new TreeMap<>();
			TreeMap<Boolean, ArrayList<Long>> delIdx = new TreeMap<>();
			TreeMap<Boolean, ArrayList<Long>> doubleIdx = new TreeMap<>();
			TreeMap<Boolean, ArrayList<Long>> cntrlIdx = new TreeMap<>();
			TreeMap<Boolean, ArrayList<Long>> govIdx = new TreeMap<>();
			TreeMap<Boolean, ArrayList<Long>> fdaIdx = new TreeMap<>();

			for (Long fv : filterValue) {
				// Sets the offset to the position filterValue - the offset
				// position, measured in bytes from the beginning of the file,
				// at which to set the file pointer.
				Long delFp = fv;
				ranFile.seek(fv);
				int Id = ranFile.readInt(); // Reads 4 bytes from the current
											// pointer -ID
				int len = ranFile.readByte(); // -Length of the Company field
				String st1 = new String();
				String st2 = new String();
				for (int j = 0; j < len; j++) {
					// ReadChar read maximum 2 bytes from the pointer
					st1 = st1 + (char) ranFile.readByte();
				}
				// Character 6 field.Drug ID
				for (int j = 0; j < 6; j++) {
					st2 = st2 + (char) ranFile.readByte();
				}
				// Get the record info from the file and assign it to each
				// variable for column
				short trials = ranFile.readShort(); // Trials
				short patients = ranFile.readShort(); // Patients
				short dosage_mg = ranFile.readShort(); // Dosage
				float reading = ranFile.readFloat(); // Reading
				// Gets the pointer at deletion
				Long fp = ranFile.getFilePointer();
				byte boolVal = ranFile.readByte();
				boolVal = (byte) (boolVal | delete_flag);
				// Sets pointer at deletion
				ranFile.seek(fp);
				// Mark deletion indicator as true
				ranFile.writeByte(boolVal);
				// ranFile.writeBoolean(true);
				/** To Update Deletion Index file ****/

				deletionIdx.clear();
				// Check if the Index file is present
				if (!idx_files[7].isFile()) {
					System.err.println("Index File Missing");
					System.exit(1);
				}
				Properties prp = new Properties();
				char flag_true = 0;
				// Loads the properties of the Idx file Input stream
				prp.load(new FileInputStream(idx_files[7]));
				for (Object keys : prp.keySet()) {
					// Get the key values in Index file separated by comma in to
					// the
					// string array
					String[] values = prp.get(keys).toString().split(",");
					ArrayList<Long> record = new ArrayList<Long>();
					for (String val : values) {
						// If the value is not deleted currently
						if (Long.parseLong(val) != delFp) {
							record.add(Long.parseLong(val));
						}
					}
					// Add the key and values to the Hash
					String keyVal = keys.toString();
					// Check if already true index is present
					if (keyVal.equalsIgnoreCase(new String("true"))) {
						record.add(fv);
						flag_true = 'X';
					}
					deletionIdx.put(keys, record);
				}
				// To consider first deletion incident
				if (flag_true != 'X') {
					ArrayList<Long> record = new ArrayList<Long>();
					record.add(fv);
					Object keyTrue = new Object();
					keyTrue = "true";
					deletionIdx.put(keyTrue, record);
				}

				for (int i = 0; i <= 11; i++) {
					if (i != 7) {
						TreeMap<Object, ArrayList<Long>> recordIdx = new TreeMap<>();
						recordIdx.clear();
						// Check if the Index file is present
						if (!idx_files[i].isFile()) {
							System.err.println("Index File Missing");
							System.exit(1);
						}
						Properties prp1 = new Properties();
						// Loads the properties of the Idx file Input stream
						prp1.load(new FileInputStream(idx_files[i]));
						if (i == 0) {
							for (Object keys : prp1.keySet()) {
								// Get the key values in Index file separated by
								// comma in to the
								// string array
								String val = prp1.get(keys).toString();
								String key = (String) keys;
								int keyVal = Integer.parseInt(key);
								Long rec = null;
								// If the value is not deleted currently
								if (!val.isEmpty()) {
									if (Long.parseLong(val) != delFp) {
										rec = Long.parseLong(val);
									}
									// If the assignment is not null
									if (rec != null)
										idIdx.put(keyVal, rec);
								}
							}
						} else {
							for (Object keys : prp1.keySet()) {
								// Get the key values in Index file separated by
								// comma in to the
								// string array
								String[] values = prp1.get(keys).toString().split(",");
								ArrayList<Long> record = new ArrayList<Long>();
								String key = (String) keys;
								for (String val : values) {
									// If the value is not deleted currently
									if (!val.isEmpty()) {
										if (Long.parseLong(val) != delFp) {
											record.add(Long.parseLong(val));
										}
									}
								}
								if (i == 1) {
									compIdx.put(key, record);
								} else if (i == 2) {
									drugIdx.put(key, record);
								} else if (i == 3) {
									Short keyVal = Short.parseShort(key);
									trialsIdx.put(keyVal, record);
								} else if (i == 4) {
									Short keyVal = Short.parseShort(key);
									patientIdx.put(keyVal, record);
								} else if (i == 5) {
									Short keyVal = Short.parseShort(key);
									dosageIdx.put(keyVal, record);
								} else if (i == 6) {
									Float keyVal = Float.parseFloat(key);
									readingIdx.put(keyVal, record);
								} else if (i == 8) {
									Boolean keyVal = Boolean.parseBoolean(key);
									doubleIdx.put(keyVal, record);
								} else if (i == 9) {
									Boolean keyVal = Boolean.parseBoolean(key);
									cntrlIdx.put(keyVal, record);
								} else if (i == 10) {
									Boolean keyVal = Boolean.parseBoolean(key);
									govIdx.put(keyVal, record);
								} else if (i == 11) {
									Boolean keyVal = Boolean.parseBoolean(key);
									fdaIdx.put(keyVal, record);
								}
							}
						}
					}
				}

				writeIndices(deletionIdx, idx_files[7]);

				writeIndices(idIdx, idx_files[0]);
				writeIndices(compIdx, idx_files[1]);
				writeIndices(drugIdx, idx_files[2]);
				writeIndices(trialsIdx, idx_files[3]);
				writeIndices(patientIdx, idx_files[4]);
				writeIndices(dosageIdx, idx_files[5]);
				writeIndices(readingIdx, idx_files[6]);
				writeIndices(doubleIdx, idx_files[8]);
				writeIndices(cntrlIdx, idx_files[9]);
				writeIndices(govIdx, idx_files[10]);
				writeIndices(fdaIdx, idx_files[11]);
			}
			ranFile.close();
		}
	}

	/*** Triggering Code execution *****/

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			MyDatabase mydb = new MyDatabase();
			mydb.displayMenu();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
