import java.io.File;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedList;

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

public class CorpusStatistics{
  private static long corpus_doc_count; //represents number of documents in the corpus
  private static double corpus_avg_length;  //represents the average length of a doc in corpus
  private static final String WIKI_SMALL_INDEX_2 = "/homes/apydi/galago-3.12/wiki-small-index-2";
  private static HashMap<String, Double> _idf_values = new HashMap<String, Double>();

    /*
    * This function reports the total number of docs in corpus, avg length of docs in corpus,
    * and the longest document(s) in the corpus.
    */
  public static void print_statistics(){
    try{
      String pathIndexBase = WIKI_SMALL_INDEX_2;
      Retrieval retrieval = RetrievalFactory.instance(pathIndexBase);
      Node fieldNode = new Node();
      fieldNode.setOperator("lengths");
      fieldNode.getNodeParameters().set("part", "lengths");
      FieldStatistics stat = retrieval.getCollectionStatistics(fieldNode);

      corpus_doc_count = stat.documentCount;  //global variable initialized
      corpus_avg_length = stat.avgLength;    //global variable initialized

      long maxDocLength = stat.maxLength; //represents the maximum length of a document in the corpus

      //Finding the documents with length equal to maxLength derived above.
      DiskLengthsReader test = new DiskLengthsReader("/homes/apydi/galago-3.12/wiki-small-index-2/lengths");
      LengthsIterator length_iterator = test.getLengthsIterator();
      System.out.println("\nPrinting Documents of Maximum Length " + maxDocLength + ".");
      int count = 1;
      while(!length_iterator.isDone()){
          long doc_id = length_iterator.currentCandidate();
          int current_doc_length = retrieval.getDocumentLength(doc_id);
          if(current_doc_length == maxDocLength){
            System.out.printf("%d. Doc Id: %d \tName: %s\n", count, doc_id, retrieval.getDocumentName(doc_id));
            count++;
          }
          length_iterator.movePast(doc_id);
      }

      //Reporting the total number of documents in the corpus and avg length of docs in words
      System.out.printf("\nTotal Number of Documents in the Corpus: %d\n", corpus_doc_count);
      System.out.printf("Average Length of Docs: %f\n", corpus_avg_length);
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /*
  * This function reports the total number of unique words that appear in the corpus.
  */
  public static void find_unique_term_count(){
    try{
      String pathIndex = "/homes/apydi/galago-3.12/wiki-small-index-2/postings";
      IndexPartReader reader = DiskIndex.openIndexPart(pathIndex);
      if(reader.getManifest().get("emptyFileIndex", false)){
        System.out.println("Empty File Index.");
      }
      KeyIterator iterator = reader.getIterator();
      int result = 0;
      while(!iterator.isDone()){
        iterator.nextKey();
        result++;
      }
      reader.close();
      System.out.printf("\n\nNumber of Unique Terms %d\n", result);
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /*
  * This function takes a single word from a query string as an argument
  * and it reports the number of documents the word appears in the corpus,
  * the frequency of the word in the corpus, the maximum count of the word in a document in the corpus,
  * AND the documents that have maximum count of the word passed as the argument.
  */
  public static void print_term_statistics(String query_1){
    try{
      String pathIndexBase = WIKI_SMALL_INDEX_2;
      Retrieval r = RetrievalFactory.instance(pathIndexBase, Parameters.create());

      Node node = StructuredQuery.parse(query_1);
      node.getNodeParameters().set("queryType", "count");
      node = r.transformQuery(node, Parameters.create());

      NodeStatistics stat = r.getNodeStatistics(node);
      long maxCount = stat.maximumCount;  //represents the maximum occurence of query in a document
      long nodeDocCount = stat.nodeDocumentCount; //represents the number of docs the word appears in
      long nodeFrequency = stat.nodeFrequency;  //represents the frequency of the query

      //OBSERVE THAT IDF values are calculated and inseted into the gloabl variable _idf_values
      double idf = Math.log((corpus_doc_count - nodeDocCount + 0.5)/(nodeDocCount + 0.5));
      _idf_values.put(query_1, idf);

      System.out.println("Query: " + query_1);
      System.out.println("Maximum Count: "+maxCount);
      System.out.println("Node Document Count:  "+nodeDocCount);
      System.out.println("Node Frequency: "+nodeFrequency);
      System.out.println("IDF: " + idf);

      //Here we find the documents that have the MAXIMUM COUNT of the argument term
      System.out.println("Documents with Maximum Count " + maxCount);
      String field = "text";
      File pathPosting = new File(new File(pathIndexBase), "postings");
      DiskIndex index = new DiskIndex(pathIndexBase);
      IndexPartReader read_posting = DiskIndex.openIndexPart(pathPosting.getAbsolutePath());
      KeyIterator _vocabulary = read_posting.getIterator();
      if(_vocabulary.skipToKey(ByteUtil.fromString(query_1)) && query_1.equals(_vocabulary.getKeyString())){
        CountIterator iterator = (CountIterator) _vocabulary.getValueIterator();
        ScoringContext sc = new ScoringContext();
        while(!iterator.isDone()){
          sc.document = iterator.currentCandidate();
          int freq = iterator.count(sc);
          String docno = index.getName(sc.document);
          if(freq == maxCount){
            System.out.printf("%s \t %15s \t %d\n", sc.document, docno, freq);
          }
          iterator.movePast(iterator.currentCandidate());
        }
      }
      System.out.println("\n");
      read_posting.close();
      index.close();
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /*
  * This helper function returns a set of ALL the documents in which the
  * the word that was passed as an argument appears in.
  */
  public static Set<Long> document_set(String _query){
    try{
      HashSet<Long> _all_doc_ids = new HashSet<Long>();
      String pathIndexBase = WIKI_SMALL_INDEX_2;
      String field = "text";
      File pathPosting = new File(new File(pathIndexBase), "postings");
      DiskIndex index = new DiskIndex(pathIndexBase);
      IndexPartReader read_posting = DiskIndex.openIndexPart(pathPosting.getAbsolutePath());
      KeyIterator _vocabulary = read_posting.getIterator();
      if(_vocabulary.skipToKey(ByteUtil.fromString(_query)) && _query.equals(_vocabulary.getKeyString())){
        CountIterator iterator = (CountIterator) _vocabulary.getValueIterator();
        ScoringContext sc = new ScoringContext();
        while(!iterator.isDone()){
          sc.document = iterator.currentCandidate();
          int freq = iterator.count(sc);
          if(freq > 0 ){
            _all_doc_ids.add(sc.document);
          }
          iterator.movePast(iterator.currentCandidate());
        }
      }
      return _all_doc_ids;
    }
    catch(Exception e){
      e.printStackTrace();
      return null;
    }
  }

  /*
  * This helper function returns the number of terms in the argument document
  */
  public static int find_doc_term_count(long _doc_id){
    try{
      String pathIndexBase = WIKI_SMALL_INDEX_2;
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

  /*
  * This helper function returns the number of times the argument word
  * occurs in the argument document.
  */
  public static int find_doc_occurence_count(long _doc_id, String _word){
    try{
      String pathIndexBase = WIKI_SMALL_INDEX_2;
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
      * This method computes the TF-IDF score for given document and given query.
    **/
  public static double compute_tf_idf(long _doc_id, LinkedList<String> _query_terms){
    double result = 0.0;
    for(String _query : _query_terms){
      double _term_idf = _idf_values.get(_query);
      double _term_frequency = ((double)find_doc_occurence_count(_doc_id, _query))/ ((double)find_doc_term_count(_doc_id));
      result +=  (_term_idf * _term_frequency );
      System.out.println(_query + " " + _term_idf + " " + _term_frequency + " " + result);
    }
    return result;
  }

  /**
    * This method computes the OKAPI score for given document and given query.
  **/
  public static double compute_okapi(long _doc_id, LinkedList<String> _query_terms){
    //static class variable corpus_avg_length initialized already
    final double k_1 = 1.20;
    final double b = 0.75;
    double result = 0.0;

    for(String _query : _query_terms){
      double _term_idf = _idf_values.get(_query);
      double doc_length_in_words = (double)find_doc_term_count(_doc_id);
      double _term_frequency = ((double)find_doc_occurence_count(_doc_id, _query))/(doc_length_in_words);

      double numerator = _term_frequency * (k_1 + 1.0);
      double denominator = _term_frequency + (k_1*(1-b + (b * (doc_length_in_words/corpus_avg_length)))) ;
      result +=  _term_idf*(numerator/denominator);
      System.out.println(_query + " " + numerator + " " + denominator + " " + result);
    }
    return result;
  }

  public static void main(String[] args){
    try{
      String pathIndexBase = WIKI_SMALL_INDEX_2;
      print_statistics();

      print_term_statistics("probabilistic");
      print_term_statistics("analysis");
      print_term_statistics("decision");
      print_term_statistics("model");
      print_term_statistics("entropy");
      print_term_statistics("science");

      find_unique_term_count();

      //We use the DOCUMENT_SETS and the retainAll function in Sets to find
      //documents that have concurrent occurences of words.
      Set<Long> result_1 = document_set("probabilistic");
      Set<Long> result_2 = document_set("analysis");
      Set<Long> result_3 = document_set("decision");
      Set<Long> result_4 = document_set("model");
      //FIND THE INTERSECTION
      result_1.retainAll(result_2);
      result_1.retainAll(result_3);
      result_1.retainAll(result_4);
      System.out.println("Printing Document Size and Names that contain: probabilistic, analysis, decision, model.");
      System.out.println(result_1.size());
      DiskIndex index = new DiskIndex(pathIndexBase);
      for(long doc_id : result_1){
        System.out.println(index.getName(doc_id));
      }

      System.out.println("\nPrinting Document Size and Names that contain: probabilistic, model.");
      result_1 = document_set("probabilistic"); //observe that we initialized again
      result_4 = document_set("model");
      result_1.retainAll(result_4);
      System.out.println(result_1.size());
      for(long doc_id : result_1){
        int count_1 = find_doc_occurence_count(doc_id, "probabilistic");
        int count_2 = find_doc_occurence_count(doc_id, "model");
        System.out.println(doc_id + "\t" + index.getName(doc_id) + "\t" + count_1 + "\t" + count_2 + "\t" + (count_1+count_2));
      }
      //We find the required document id used below by observing previous output
      LinkedList<String> _query_terms = new LinkedList<String>();
      _query_terms.add("probabilistic");
      _query_terms.add("model");
      System.out.println("TF-IDF FOR DOC 2057 (probabilistic, model)" + compute_tf_idf(2057, _query_terms));

      System.out.println("\nPrinting Document Size and Names that contain: entropy, science.");
      result_1 = document_set("entropy");
      result_2 = document_set("science");
      result_1.retainAll(result_2);
      for(long doc_id : result_1){
        int count_1 = find_doc_occurence_count(doc_id, "entropy");
        int count_2 = find_doc_occurence_count(doc_id, "science");
        System.out.println(doc_id + "\t" + index.getName(doc_id) + "\t" + count_1 + "\t" + count_2 + "\t" + (count_1+count_2));
      }
      _query_terms = new LinkedList<String>();
      _query_terms.add("entropy");
      _query_terms.add("science");
      System.out.println("OKAPI FOR DOC 3285 (probabilistic, model)" + compute_okapi(3285, _query_terms));
      index.close();
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
}
