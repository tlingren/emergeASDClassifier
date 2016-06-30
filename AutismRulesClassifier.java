package org.apache.ctakes.emerge.rules;

/**
 * This class receives as input dicationary files, 
 * 
 * @author Pei Chen
 * @author Todd Lingren
 * This work produced by Cincinnati Children’s Hospital Medical Center and Boston Children’s Hospital and 
 * was supported by collaborative sites in the eMERGE Network and funded by NHGRI (U01HG006828)
 *  
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AutismRulesClassifier {

	public static final ArrayList<String> ASD_SOCIAL_CUIS = new ArrayList<String>();
	public static final ArrayList<String> ASD_COMM_CUIS = new ArrayList<String>();
	public static final ArrayList<String> ASD_BEHAVIOR_CUIS = new ArrayList<String>();
	public static final ArrayList<String> ASD_MENTAL_CUIS = new ArrayList<String>();
	
	//Explict mention of Disease/Disorder CUIS
	public static final ArrayList<String> ASPERGERS_CUIS = new ArrayList<String>();
	public static final ArrayList<String> AUTISM_CUIS = new ArrayList<String>();
	public static final ArrayList<String> PDD_CUIS = new ArrayList<String>();
	
	public static final String ASD_SOCIAL = "3.1";
	public static final String ASD_COMM = "3.2";
	public static final String ASD_BEHAVIOR = "3.3";
	public static final String ASD_MENTAL = "3.4";
	
	public static final String ASD_ASPERGERS_TUI = "3.6";
	public static final String ASD_AUTISM_TUI = "3.8";
	public static final String ASD_PDD_TUI = "3.7";
	
	//Dictionaries in Pipe-Delimited files, fields; CUI|FWORD|TEXT|CODE|SOURCETYPE|TUI
	public static final String DICTIONARY_PATH = "file:emergeCuiTable.bar";
	public static final String DICTIONARY_MR_PATH = "file:mentalRetardationCuiTable.bar";
	public static final String DICTIONARY_ASPERGERS_PATH = "file:AspergersCuiTable.bar";
	public static final String DICTIONARY_AUTISM_PATH = "file:AutismCuiTable.bar";
	public static final String DICTIONARY_PDD_PATH = "file:PDDCuiTable.bar";	
	
	
	//input file is a patient vector of CUIS
	public static final String VECTOR_PATH = "/data/knowtator/shared/emerge/emerge_svn/cchmc_vectors/cchmc_40_all.vectors";

	public static final Properties DB_PROPS = new Properties();
	public static final String DB_PROPS_PATH = "CTAKES_PATH/src/main/resources/org/apache/ctakes/emerge/DBProperties.properties";

	public static final String CLASS_ASD = "ASD";
	public static final String CLASS_ASPERGER = "ASPERGER";
	public static final String CLASS_PDD_NOS = "PDD-NOS";
	public static final String CLASS_NONE = "NONE";
	public static final String LANGAGUE_DELAY_CUI = "C0023012";

	public static void main(String args[]) throws Exception {

		//for classifyFromDB
		DB_PROPS.load(AutismRulesClassifier.class.getClassLoader()
				.getResourceAsStream(DB_PROPS_PATH));

		loadASDCUIDataDictionary(DICTIONARY_PATH);
		loadASDCUIDataDictionary(DICTIONARY_MR_PATH);
		loadASDCUIDataDictionary(DICTIONARY_ASPERGERS_PATH);
		loadASDCUIDataDictionary(DICTIONARY_AUTISM_PATH);
		loadASDCUIDataDictionary(DICTIONARY_PDD_PATH);

		//Uncomment One method to run
		//classifyFromDB();
		//classifyFromFile();

	}
	/**
	 * Expected Vector file format:
	 * PAT_ID C01234:1 LABEL
	 * PAT_ID is the CCHMC id of the patient e.g. 0-39.
	 * C01234 is the cui
	 * 1 indicates # of occurrences in all notes
	 * LABEL is integer 0-3:
	  * 0 Yes-ASD 
	  * 1 No 
	  * 2 Maybe 
	  * 3 Unknown
	 */
	private static void classifyFromFile() throws Exception {
		
		FileReader f = new FileReader(VECTOR_PATH);
		BufferedReader input = new BufferedReader(f);
		try {
			String line = null;
			while ((line = input.readLine()) != null) {
				String[] vectors = line.split(" ");
				String patient = null;
				String label = null;
				int social_count = 0;
				int comm_count = 0;
				int behavior_count = 0;
				int language_delay = 0;
				int avg_cog_function = 0;
				
				int autism_count = 0;
				int aspergers_count = 0;
				int pdd_count = 0;

				if (vectors.length > 0) {
					patient = vectors[0];
					label = vectors[vectors.length - 1];

					for (int i = 1; i < vectors.length - 1; i++) {
						// these are the cui:counts
						String[] cuicount = vectors[i].split(":");
						String cui = cuicount[0];
						String count = cuicount[1];
						int temp = Integer.parseInt(count);
						if (ASD_SOCIAL_CUIS.contains(cui.trim().toUpperCase())) {
							// TODO: Need to check "No Language Delay
							// And Avg. Cognitive Function
							social_count += temp;
						}
						if (ASD_COMM_CUIS.contains(cui.trim().toUpperCase())) {
							comm_count += temp;
						}
						if (ASD_BEHAVIOR_CUIS
								.contains(cui.trim().toUpperCase())) {
							behavior_count += temp;
						}
						if (ASD_MENTAL_CUIS.contains(cui.trim().toUpperCase())) {
							avg_cog_function += temp;
						}
						if (LANGAGUE_DELAY_CUI.equalsIgnoreCase(cui.trim())) {
							language_delay++;
						}
						if (AUTISM_CUIS.contains(cui.trim().toUpperCase())) {
							autism_count += temp;
						}		
						if (ASPERGERS_CUIS.contains(cui.trim().toUpperCase())) {
							aspergers_count += temp;
						}
						if (PDD_CUIS.contains(cui.trim().toUpperCase())) {
							pdd_count += temp;
						}								
					}
					String classification = classifyASD(social_count,
							comm_count, behavior_count, language_delay,
							avg_cog_function,autism_count, aspergers_count, pdd_count);
					System.out.println("Patient:" + patient + " social:"
							+ social_count + " comm:" + comm_count
							+ " behavior:" + behavior_count
							+ " language_delay:" + language_delay
							+ " avg_cog_fn:" + avg_cog_function
							+ " autism_count:" + autism_count 
							+ " aspergers_count:" + aspergers_count 
							+ " pdd_count:" + pdd_count 							
							+ " class:" + classification + " Gold Label:" + label);
				}
			}

		} finally {
			input.close();
		}

	}
	/**
	 * Classification ASD into sublcass (CASE) or noncase (NONE)
	 * @param social
	 * @param comm
	 * @param behavior
	 * @param language_delay
	 * @param mental
	 * @param autism_count
	 * @param aspergers_count
	 * @param pdd_count
	 * @return
	 */
	private static String classifyASD(int social, int comm, int behavior,
			int language_delay, int mental, int autism_count, 
			int aspergers_count, int pdd_count) {
		String classification = null;
		// CASE Autistic Disorder
		if (social >= 2 && comm >= 1 && behavior >= 1
				&& (social + comm + behavior + autism_count >= 6)) {
			classification = CLASS_ASD;
		}
		// CASE Asperger's
		else if ( ((social + aspergers_count >= 2 && behavior + aspergers_count >= 1) ) && (mental == 0 && language_delay == 0)) {
			classification = CLASS_ASPERGER;
		}
		// CASE PDD-NOS
		else if ((social >= 1 || comm >= 1 || behavior >= 1 || pdd_count >= 1)) {
			classification = CLASS_PDD_NOS;
		}
		// CASE NONE
		else {
			classification = CLASS_NONE;
		}
		return classification;
	}

	private static void loadASDCUIDataDictionary(String path)
			throws IOException {
		System.out.println("Loading Dictionary:" + path);
		File f = new File(path);
		URL url = new URL(f.getPath());
		InputStream social = url.openStream();
		BufferedReader in = new BufferedReader(new InputStreamReader(social));
		String line = null;
		while ((line = in.readLine()) != null) {
			String[] l = line.split("\\|");
			if (l != null && l.length > 0) {
				String cui = l[0];
				String tui = l[5];
				if (tui != null && ASD_SOCIAL.equalsIgnoreCase(tui)) {
					ASD_SOCIAL_CUIS.add(cui.trim().toUpperCase());
				} else if (tui != null && ASD_COMM.equalsIgnoreCase(tui)) {
					ASD_COMM_CUIS.add(cui.trim().toUpperCase());
				} else if (tui != null && ASD_BEHAVIOR.equalsIgnoreCase(tui)) {
					ASD_BEHAVIOR_CUIS.add(cui.trim().toUpperCase());
				} else if (tui != null && ASD_MENTAL.equalsIgnoreCase(tui)) {
					ASD_MENTAL_CUIS.add(cui.trim().toUpperCase());
				} else if (tui != null && ASD_ASPERGERS_TUI.equalsIgnoreCase(tui)) {
					ASPERGERS_CUIS.add(cui.trim().toUpperCase());					
				} else if (tui != null && ASD_AUTISM_TUI.equalsIgnoreCase(tui)) {
					AUTISM_CUIS.add(cui.trim().toUpperCase());		
				} else if (tui != null && ASD_PDD_TUI.equalsIgnoreCase(tui)) {
					PDD_CUIS.add(cui.trim().toUpperCase());							
				} else {
					System.out.println("TUI " + tui + " not recognized");
				}
			}
		}
	}

	private static void classifyFromDB() {
		List<String> patients = getPatientsFromDB();
		for (String patient : patients) {
			int social_count = 0;
			int comm_count = 0;
			int behavior_count = 0;
			int language_delay = 0;
			int avg_cog_function = 0;
			
			int autism_count = 0;
			int aspergers_count = 0;
			int pdd_count = 0;
			
			List<String> cuis = getCUISforPatientFromDB(patient);
			for (String cui : cuis) {
				if (ASD_SOCIAL_CUIS.contains(cui.trim().toUpperCase())) {
					// TODO: Need to check "No Language Delay
					// And Avg. Cognitive Function
					social_count ++;
				}
				if (ASD_COMM_CUIS.contains(cui.trim().toUpperCase())) {
					comm_count ++;
				}
				if (ASD_BEHAVIOR_CUIS
						.contains(cui.trim().toUpperCase())) {
					behavior_count ++;
				}
				if (ASD_MENTAL_CUIS.contains(cui.trim().toUpperCase())) {
					avg_cog_function ++;
				}
				if (LANGAGUE_DELAY_CUI.equalsIgnoreCase(cui.trim())) {
					language_delay++;
				}
				if (AUTISM_CUIS.contains(cui.trim().toUpperCase())) {
					autism_count ++;
				}		
				if (ASPERGERS_CUIS.contains(cui.trim().toUpperCase())) {
					aspergers_count ++;
				}
				if (PDD_CUIS.contains(cui.trim().toUpperCase())) {
					pdd_count ++;
				}					
			}

			String classification = classifyASD(social_count,
					comm_count, behavior_count, language_delay,
					avg_cog_function,autism_count, aspergers_count, pdd_count);
			System.out.println("Patient:" + patient + " social:"
					+ social_count + " comm:" + comm_count
					+ " behavior:" + behavior_count
					+ " language_delay:" + language_delay
					+ " avg_cog_fn:" + avg_cog_function
					+ " autism_count:" + autism_count 
					+ " aspergers_count:" + aspergers_count 
					+ " pdd_count:" + pdd_count 							
					+ " class:" + classification );
		}
	}

	private static List<String> getPatientsFromDB() {
		ArrayList<String> patients = new ArrayList<String>();
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection(
					DB_PROPS.getProperty("DATABASE_DRIVER"),
					DB_PROPS.getProperty("DATABASE_URL"),
					DB_PROPS.getProperty("DATABASE_USER"),
					DB_PROPS.getProperty("DATABASE_PASS"));

			final String sql = "select distinct patient_num from "
					+ DB_PROPS.getProperty("VECTORS_TABLE");
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			while (rs.next()) {
				String patient = rs.getString("patient_num");
				patients.add(patient);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return patients;
	}

	private static List<String> getCUISforPatientFromDB(String patient_num) {
		ArrayList<String> cuis = new ArrayList<String>();
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection(
					DB_PROPS.getProperty("DATABASE_DRIVER"),
					DB_PROPS.getProperty("DATABASE_URL"),
					DB_PROPS.getProperty("DATABASE_USER"),
					DB_PROPS.getProperty("DATABASE_PASS"));
			// We only care about the CUI's; ignore the snomed codes
			final String sql = "select distinct concept_cd, concept_polarity from "
					+ DB_PROPS.getProperty("VECTORS_TABLE")
					+ " where patient_num = ? and concept_cd like 'C%'";
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, patient_num);
			rs = stmt.executeQuery();
			while (rs.next()) {
				String concept_cd = rs.getString("concept_cd");
				int polarity = rs.getInt("concept_polarity");
				cuis.add((polarity < 0 ? "-" : "") + concept_cd);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return cuis;
	}
}
