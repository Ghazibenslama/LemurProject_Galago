import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.Comparator;
import java.util.Arrays;

import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.parse.Document;

import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.KeyIterator;

import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.index.ExtractIndexDocumentNumbers;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskLengthsIterator;
import org.lemurproject.galago.utility.Parameters;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;

import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.utility.ByteUtil;

public class CACMStat{
  private static long corpus_doc_count; //represents number of documents in the corpus
  private static double corpus_avg_length;  //represents the average length of a doc in corpus

  private static final String CACM_INDEX_PATH = "/homes/apydi/galago-3.12/cacm-index-path";
  private static final String CACM_REL_DOC_PATH = "/homes/apydi/galago-3.12/cacm.rel";
  private static final String CACM_TOP_100_DOC_PATH = "/homes/apydi/galago-3.12/top-100.txt";
  private static HashMap<String, Double> _idf_values = new HashMap<String, Double>();

  //The RELEVANT document sets are initialized using the relevance judgements file
  //provided on the textbook website
  private static HashSet<Long> _relevantDocSet_query4 = new HashSet<Long>();
  private static HashSet<Long> _relevantDocSet_query7 = new HashSet<Long>();
  private static HashSet<Long> _relevantDocSet_query16 = new HashSet<Long>();
  private static HashSet<Long> _relevantDocSet_query18 = new HashSet<Long>();

  //The TOP 100 document sets corresponding to each query are populated using the
  //output from the galago batch search in the previous part.
  private static HashSet<Long> _top100_query4_DocSet = new HashSet<Long>();
  private static HashSet<Long> _top100_query7_DocSet = new HashSet<Long>();
  private static HashSet<Long> _top100_query16_DocSet = new HashSet<Long>();
  private static HashSet<Long> _top100_query18_DocSet = new HashSet<Long>();

    /**
      * Populates the TOP 100 document sets with the corresponding data
      * The output from the batch-search is used to populate the document sets
    **/
  public static void initializeTOP100DocSet(){
    try{
      String pathIndexBase = CACM_INDEX_PATH;
      BufferedReader br = new BufferedReader(new FileReader(CACM_TOP_100_DOC_PATH));
      String _currentLine;
      DiskIndex index = new DiskIndex(pathIndexBase);

      //Loops through all the lines in the input file
      while((_currentLine = br.readLine()) != null){
        int count = 0;  //used to find the tokens we are interested in. Relies on document format
        long doc_id = -1;
        int query_number = -1;
        //loops through tokens (the words) in each line
        for(String s : get_query_terms(_currentLine)){
          if(count == 0){ //remember the 0'th token is the query number
            query_number = Integer.parseInt(s);
          }
          if(count == 2){ // the 2'th token is the absolute address of the file
            doc_id = index.getIdentifier(s);
          }
          count++;
        }
        switch(query_number){
          case 4: _top100_query4_DocSet.add(doc_id);
                  break;
          case 7: _top100_query7_DocSet.add(doc_id);
                  break;
          case 16:  _top100_query16_DocSet.add(doc_id);
                    break;
          case 18:  _top100_query18_DocSet.add(doc_id);
                    break;
          default: break;
        }
      }
      index.close();
      br.close();
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /**
    * Populates the RELEVANT document sets with the corresponding data
    * The RELEVANCE JUDGEMENTS file given to us on the book website is used to populate the sets.
  **/
  public static void initializeRelevantDocSet(){
    try{
      String pathIndexBase = CACM_INDEX_PATH;
      BufferedReader br = new BufferedReader(new FileReader(CACM_REL_DOC_PATH));
      String _currentLine;
      DiskIndex index = new DiskIndex(pathIndexBase);

      while((_currentLine = br.readLine()) != null){  //loops through each line in relevance judgements file
        int count = 0;  //used to identify the words in each line we are interested in extracting
        long doc_id = -1;
        int query_number = -1;
        for(String s : get_query_terms(_currentLine)){ //loops through tokens (the words) in each line
          if(count == 0){ // query number is the first word in each line
            query_number = Integer.parseInt(s);
          }
          if(count == 2){ //the file name is the third word in each line
            doc_id = index.getIdentifier("/home/u96/apydi/galago-3.12/cacm/"+s+".html");
          }
          count++;
        }
        switch(query_number){
          case 4: _relevantDocSet_query4.add(doc_id);
                  break;
          case 7: _relevantDocSet_query7.add(doc_id);
                  break;
          case 16:  _relevantDocSet_query16.add(doc_id);
                    break;
          case 18:  _relevantDocSet_query18.add(doc_id);
                    break;
          default: break;
        }
      }
      index.close();
      br.close();
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /**
    * This method is used to intialize the global variables corpus_doc_count
    * AND corpus_avg_length.
  **/
  public static void initializeGlobal(){
    try{
      String pathIndexBase = CACM_INDEX_PATH;
      Retrieval retrieval = RetrievalFactory.instance(pathIndexBase);
      Node fieldNode = new Node();
      fieldNode.setOperator("lengths");
      fieldNode.getNodeParameters().set("part", "lengths");
      FieldStatistics stat = retrieval.getCollectionStatistics(fieldNode);
      corpus_doc_count = stat.documentCount;
      corpus_avg_length = stat.avgLength;
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /**
    * This method is used to initialize the global variable _idf_values (HASHMAP<String, Double>)
    * The IDF values of all the terms in all the queries are populated through this function.
  **/
  public static void intializeIDF(LinkedList<String> _query_terms){
    try{
      String pathIndexBase = CACM_INDEX_PATH;
      Retrieval r = RetrievalFactory.instance(pathIndexBase, Parameters.create());

      for(String _query : _query_terms){  //iterate through each term in the query
        Node node = StructuredQuery.parse(_query);
        node.getNodeParameters().set("queryType", "count");
        node = r.transformQuery(node, Parameters.create());
        NodeStatistics stat = r.getNodeStatistics(node);
        long nodeDocCount = stat.nodeDocumentCount;
        double idf = Math.log((corpus_doc_count - nodeDocCount + 0.5)/(nodeDocCount + 0.5)); //observe use of global variable
        _idf_values.put(_query, idf); //IDF value inserted in global HASHMAP
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /**
    * This method computes the TF-IDF score for given document and given query.
  **/
  public static double compute_tf_idf(long _doc_id, LinkedList<String> _query_terms){
    double result = 0.0;
    for(String _query : _query_terms){
      double _term_idf = _idf_values.get(_query);
      double _term_frequency = ((double)find_doc_occurence_count(_doc_id, _query))/ ((double)find_doc_term_count(_doc_id));
      result +=  (_term_idf * _term_frequency );
    }
    return result;
  }

  /**
    * This method computes the Okapi score for given document and given query.
  **/
  public static double compute_okapi(long _doc_id, LinkedList<String> _query_terms){
    //REMEMBER static class variable corpus_avg_length initialized already
    final double k_1 = 1.20;
    final double b = 0.75;
    double result = 0.0;

    for(String _query : _query_terms){
      double _term_idf = _idf_values.get(_query); //observe idf values already populated
      double doc_length_in_words = (double)find_doc_term_count(_doc_id); //helper method used
      double _term_frequency = ((double)find_doc_occurence_count(_doc_id, _query))/(doc_length_in_words); //helper method used

      double numerator = _term_frequency * (k_1 + 1.0);
      double denominator = _term_frequency + (k_1*(1-b + (b * (doc_length_in_words/corpus_avg_length)))) ;
      result +=  _term_idf*(numerator/denominator);
    }
    return result;
  }

  /**
    * Helper method that returns the size of the document in words.
  **/
  public static int find_doc_term_count(long _doc_id){
    try{
      String pathIndexBase = CACM_INDEX_PATH;
      Retrieval retrieval = RetrievalFactory.instance(pathIndexBase);
      Document.DocumentComponents dc = new Document.DocumentComponents(false, false, true);
      Document doc = retrieval.getDocument(retrieval.getDocumentName(_doc_id), dc);
      return doc.terms.size();
    }
    catch(Exception e){
        e.printStackTrace();
        return -1;
    }
  }

  /**
    * Helper method that counts the number of times a word in a given document.
  **/
  public static int find_doc_occurence_count(long _doc_id, String _word){
    try{
      String pathIndexBase = CACM_INDEX_PATH;
      Retrieval retrieval = RetrievalFactory.instance(pathIndexBase);
      Document.DocumentComponents dc = new Document.DocumentComponents(false, false, true);
      Document doc = retrieval.getDocument(retrieval.getDocumentName(_doc_id), dc);
      int result = 0;
      for(String term : doc.terms){
        if(term.equals(_word)){
          result++;
        }
      }
      return result;
    }
    catch(Exception e){
        e.printStackTrace();
        return -1;
    }
  }

  /**
    * Helper method that tokenizes a query into its constituent words (separator is white space).
  **/
  public static LinkedList<String> get_query_terms(String _query){
    LinkedList<String> result = new LinkedList<String>();
    StringTokenizer st = new StringTokenizer(_query);
    while(st.hasMoreTokens()){
      result.add(st.nextToken());
    }
    return result;
  }

  /**
    * The required Eval data is evaluated and printed in this function.
  **/
  public static void printEvalData(DocumentScore[] _docScoresArray, HashSet<Long> _relevantDocSet, HashSet<Long> _top100){
    //MAP
    int relevantDocsFound = 0;
    double averagePrecision = 0.0;
    for(int i = 0; i < _docScoresArray.length; i++){
      if(_relevantDocSet.contains(_docScoresArray[i]._doc_id)){
        relevantDocsFound++;
        averagePrecision += (((double)relevantDocsFound)/((double)(i+1)));
      }
    }
    averagePrecision = averagePrecision / ((double)relevantDocsFound);
    System.out.println("-----------------------------MAP:\t"+ averagePrecision);

    //Normalized Discounted Cumulative Gain
    HashSet<Long> intersection = new HashSet<Long>(_relevantDocSet);
    intersection.retainAll(_top100);
    int relevantCount = intersection.size();
    double dcg = 0.0;
    double ideal_dcg = 0.0;
    double n_dcg = 0.0;
    int ideal_tracker = 0;
    for(int i = 0; i < _docScoresArray.length; i++){
      int rank = i + 1;
      double relevance = (_relevantDocSet.contains(_docScoresArray[i]._doc_id))?1.0:0.0;
      if(rank == 1){
        dcg += relevance;
        if(ideal_tracker < relevantCount){
          ideal_dcg += 1.0;
          ideal_tracker++;
        }
      }
      else{
        if(relevance == 0.0){
          dcg += relevance;
          if(ideal_tracker < relevantCount){
            ideal_dcg += (1.0/(Math.log(rank)/Math.log(2)));
            ideal_tracker++;
          }
        }
        else{
          dcg += (relevance/(Math.log(rank)/Math.log(2)));
          if(ideal_tracker < relevantCount){
            ideal_dcg += (1.0/(Math.log(rank)/Math.log(2)));
          }
        }
      }
      n_dcg = (dcg/ideal_dcg);
    }
    System.out.println("----------N_DCG--(DCG, IDEAL_DCG):\t" + n_dcg + "\t(" + dcg + "," + ideal_dcg+")");

    //precision @ 20
    double count = 0.0;
    for(int i = 0; i < 20; i++){
      if(_relevantDocSet.contains(_docScoresArray[i]._doc_id)){
        count = count + 1.0;
      }
    }
    System.out.println("-----------------------------P@20:\t" + (count/20.0));
  }

  public static void main(String[] args){
    try{
      String pathIndexBase = CACM_INDEX_PATH;
      initializeGlobal();
      String query_4 = "im interested in mechanisms for communicating between disjoint processes possibly  but not exclusively  in a distributed environment   i would rather see descriptions of complete mechanisms  with or without implementations as opposed to theoretical work on the abstract problem   remote procedure calls and message passing are examples of my interests   ";
      String query_7 = "  i am interested in distributed algorithms   concurrent programs in which processes communicate and synchronize by using message passing areas of particular interest include fault tolerance and techniques for understanding the correctness of these algorithms   ";
      String query_16 = "  find all descriptions of file handling in operating systems based on multiple processes and message passing   ";
      String query_18 = "  languages and compilers for parallel processors  especially highly horizontal microcoded machines  code compaction   ";

      LinkedList<String> query_4_terms = get_query_terms(query_4);
      LinkedList<String> query_7_terms = get_query_terms(query_7);
      LinkedList<String> query_16_terms = get_query_terms(query_16);
      LinkedList<String> query_18_terms = get_query_terms(query_18);

      intializeIDF(query_4_terms);
      intializeIDF(query_7_terms);
      intializeIDF(query_16_terms);
      intializeIDF(query_18_terms);

      initializeRelevantDocSet();
      initializeTOP100DocSet();

      DiskIndex index = new DiskIndex(pathIndexBase);

      DocumentScore[] _query4_doc_scores = new DocumentScore[100];
      int i = 0;
      for (Long doc_id : _top100_query4_DocSet){
        _query4_doc_scores[i] = new DocumentScore(doc_id, compute_tf_idf(doc_id, query_4_terms), compute_okapi(doc_id, query_4_terms) );
        i++;
      }
      System.out.println("\n\n\nQUERY 4: Sorted by TFIDF. Descending Order. (Highest --> Lowest)");
      Arrays.sort(_query4_doc_scores, new TFIDF_Sort_Comparator());
      printEvalData(_query4_doc_scores, _relevantDocSet_query4, _top100_query4_DocSet);
      System.out.println("\nQUERY 4: Sorted by Okapi. Descending Order. (Highest --> Lowest)");
      Arrays.sort(_query4_doc_scores, new Okapi_Sort_Comparator());
      printEvalData(_query4_doc_scores, _relevantDocSet_query4, _top100_query4_DocSet);

      DocumentScore[] _query7_doc_scores = new DocumentScore[100];
      i = 0;
      for (Long doc_id : _top100_query7_DocSet){
        _query7_doc_scores[i] = new DocumentScore(doc_id, compute_tf_idf(doc_id, query_7_terms), compute_okapi(doc_id, query_7_terms));
        i++;
      }
      System.out.println("\n\n\nQUERY 7: Sorted by TFIDF. Descending Order. (Highest --> Lowest)");
      Arrays.sort(_query7_doc_scores, new TFIDF_Sort_Comparator());
      printEvalData(_query7_doc_scores, _relevantDocSet_query7, _top100_query7_DocSet);
      System.out.println("\nQUERY 7: Sorted by Okapi. Descending Order. (Highest --> Lowest)");
      Arrays.sort(_query7_doc_scores, new Okapi_Sort_Comparator());
      printEvalData(_query7_doc_scores, _relevantDocSet_query7, _top100_query7_DocSet);

      DocumentScore[] _query16_doc_scores = new DocumentScore[100];
      i = 0;
      for (Long doc_id : _top100_query16_DocSet){
        _query16_doc_scores[i] = new DocumentScore(doc_id, compute_tf_idf(doc_id, query_16_terms), compute_okapi(doc_id, query_16_terms));
        i++;
      }
      System.out.println("\n\n\nQUERY 16: Sorted by TFIDF. Descending Order. (Highest --> Lowest)");
      Arrays.sort(_query16_doc_scores, new TFIDF_Sort_Comparator());
      printEvalData(_query16_doc_scores, _relevantDocSet_query16, _top100_query16_DocSet);
      System.out.println("\nQUERY 16: Sorted by Okapi. Descending Order. (Highest --> Lowest)");
      Arrays.sort(_query16_doc_scores, new Okapi_Sort_Comparator());
      printEvalData(_query16_doc_scores, _relevantDocSet_query16, _top100_query16_DocSet);

      DocumentScore[] _query18_doc_scores = new DocumentScore[100];
      i = 0;
      for (Long doc_id : _top100_query18_DocSet){
        _query18_doc_scores[i] = new DocumentScore(doc_id, compute_tf_idf(doc_id, query_18_terms), compute_okapi(doc_id, query_18_terms));
        i++;
      }
      System.out.println("\n\n\nQUERY 18: Sorted by TFIDF. Descending Order. (Highest --> Lowest)");
      Arrays.sort(_query18_doc_scores, new TFIDF_Sort_Comparator());
      printEvalData(_query18_doc_scores, _relevantDocSet_query18, _top100_query18_DocSet);
      System.out.println("\nQUERY 18: Sorted by Okapi. Descending Order. (Highest --> Lowest)");
      Arrays.sort(_query18_doc_scores, new Okapi_Sort_Comparator());
      printEvalData(_query18_doc_scores, _relevantDocSet_query18, _top100_query18_DocSet);

      index.close();
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
}
/**
  * This class is used to represent a document and its TF-IDF  and OKAPI scores.
**/
class DocumentScore{
  long _doc_id;
  double _tf_idf_score;
  double _okapi_score;

  public DocumentScore(long doc_id, double tf_idf_score, double okapi_score){
      this._doc_id = doc_id;
      this._tf_idf_score = tf_idf_score;
      this._okapi_score = okapi_score;
  }
}
class Okapi_Sort_Comparator implements Comparator<DocumentScore>{
  public int compare(DocumentScore d1, DocumentScore d2){
    //DESCENDING
    if(d1._okapi_score > d2._okapi_score){
      return -1;
    }
    else if(d1._okapi_score < d2._okapi_score){
      return 1;
    }
    else{
      return 0;
    }
  }
}
class TFIDF_Sort_Comparator implements Comparator<DocumentScore>{
  public int compare(DocumentScore d1, DocumentScore d2){
    //DESCENDING
    if(d1._tf_idf_score > d2._tf_idf_score){
      return -1;
    }
    else if(d1._tf_idf_score < d2._tf_idf_score){
      return 1;
    }
    else{
      return 0;
    }
  }
}
