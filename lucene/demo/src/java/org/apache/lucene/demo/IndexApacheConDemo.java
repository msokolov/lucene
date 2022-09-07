/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class IndexApacheConDemo implements AutoCloseable {
  static final String KNN_DICT = "knn-dict";

  // Calculates embedding vectors for KnnVector search
  private final DemoEmbeddings demoEmbeddings;
  private final KnnVectorDict vectorDict;

  private IndexApacheConDemo(KnnVectorDict vectorDict) throws IOException {
    if (vectorDict != null) {
      this.vectorDict = vectorDict;
      demoEmbeddings = new DemoEmbeddings(vectorDict);
    } else {
      this.vectorDict = null;
      demoEmbeddings = null;
    }
  }

  /** Index all text files under a directory. */
  public static void main(String[] args) throws Exception {
    String usage =
        "java org.apache.lucene.demo.IndexApacheConDemo"
            + " [-index INDEX_PATH] [-docs DOCS_PATH] [-knn_dict DICT_PATH]\n\n"
            + "This indexes the documents in DOCS_PATH, creating a Lucene index"
            + "in INDEX_PATH that can be searched with SearchFiles\n"
            + "IF DICT_PATH contains a KnnVector dictionary, the index will also support KnnVector search";
    String indexPath = "index";
    String docsPath = null;
    String vectorDictSource = null;
    boolean create = true;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "-index":
          indexPath = args[++i];
          break;
        case "-docs":
          docsPath = args[++i];
          break;
        case "-knn_dict":
          // Note: must be a text file + accompanying bin file with ".vectors" extension
          vectorDictSource = args[++i];
          break;
        default:
          throw new IllegalArgumentException("unknown parameter " + args[i]);
      }
    }

    if (docsPath == null) {
      System.err.println("Usage: " + usage);
      System.exit(1);
    }

    final Path docDir = Paths.get(docsPath);
    if (!Files.isReadable(docDir)) {
      System.out.println(
          "Document directory '"
              + docDir.toAbsolutePath()
              + "' does not exist or is not readable, please check the path");
      System.exit(1);
    }

    Date start = new Date();
    try {
      System.out.println("Indexing to directory '" + indexPath + "'...");

      Directory dir = FSDirectory.open(Paths.get(indexPath));
      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

      if (create) {
        // Create a new index in the directory, removing any
        // previously indexed documents:
        iwc.setOpenMode(OpenMode.CREATE);
      } else {
        // Add new documents to an existing index:
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
      }

      // Optional: for better indexing performance, if you
      // are indexing many documents, increase the RAM
      // buffer.  But if you do this, increase the max heap
      // size to the JVM (eg add -Xmx512m or -Xmx1g):
      //
      iwc.setRAMBufferSizeMB(1000.0);

      KnnVectorDict vectorDictInstance = null;
      long vectorDictSize = 0;
      if (vectorDictSource != null) {
        // dim hardcoded to 384 cause I'm in a hurry
        System.out.println("Build KNN vector dict '" + vectorDictSource + "'...");

        KnnVectorDict.build(Paths.get(vectorDictSource), Paths.get(vectorDictSource + ".vectors"), 384, dir, KNN_DICT);
        vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
        vectorDictSize = vectorDictInstance.ramBytesUsed();
      }

      try (IndexWriter writer = new IndexWriter(dir, iwc);
          IndexApacheConDemo indexFiles = new IndexApacheConDemo(vectorDictInstance)) {
        indexFiles.indexDocs(writer, docDir);
      } finally {
        IOUtils.close(vectorDictInstance);
      }

      Date end = new Date();
      try (IndexReader reader = DirectoryReader.open(dir)) {
        System.out.println(
            "Indexed "
                + reader.numDocs()
                + " documents in "
                + (end.getTime() - start.getTime())
                + " milliseconds");
        if (reader.numDocs() > 100
            && vectorDictSize < 1_000_000
            && System.getProperty("smoketester") == null) {
          throw new RuntimeException(
              "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Indexes the given file using the given writer.
   *
   * @param writer Writer to the index where the given file/dir info will be stored
   * @param path The file to index
   * @throws IOException If there is a low-level I/O error
   */
  void indexDocs(final IndexWriter writer, Path path) throws IOException {
    assert writer.getConfig().getOpenMode() == OpenMode.CREATE;
    System.out.println("indexing docs from " + path);
    ExecutorService exec = new ThreadPoolExecutor(8, 8, 5, TimeUnit.SECONDS,
                                                  new ArrayBlockingQueue<Runnable>(16),
                                                  new ThreadPoolExecutor.CallerRunsPolicy());
    try (BufferedReader reader = Files.newBufferedReader(path)) {
      String line;
      int count = 0;
      while ((line = reader.readLine()) != null) {
        if (++count % 1000 == 0) {
          System.out.println("[" + count + "] " + line.substring(0,16));
        }
        exec.submit(new DocTask(writer, line));
      }
    }
    exec.shutdown();
  }

  class DocTask implements Runnable {
    private final IndexWriter writer;
    private final String line;
    
    DocTask(IndexWriter writer, String line) {
      this.writer = writer;
      this.line = line;
    }

    @Override
    public void run() {
      // num,asin,title,description,bullet_point,brand,color,locale
      String[] values = line.split("\t");

      // make a new, empty document
      Document doc = new Document();

      doc.add(new StoredField("asin", values[1]));
      doc.add(new StoredField("title", values[2]));
      doc.add(new StoredField("description", values[3]));
      doc.add(new StoredField("bullet_point", values[4]));
      doc.add(new StoredField("brand", values[5]));
      doc.add(new StoredField("color", values[6]));
      // Add the entire line to a field named "text"
      doc.add(new TextField("text", line, Field.Store.NO));
                                                   
      try {
        if (demoEmbeddings != null) {
          float[] vector = demoEmbeddings.computeEmbedding(line);
          doc.add(new KnnVectorField("text-vector", vector, VectorSimilarityFunction.DOT_PRODUCT));
        }

        // New index, so we just add the document (no old document can be there):
        writer.addDocument(doc);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorDict);
  }
}
