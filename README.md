Project 2
Content Based Authorship Detection using TF/IDF Scores and Cosine Similarity


The goal of this project is to build an authorship detection system that provides a ranked list of
possible authors for a document whose authorship is unknown. The system uses word uni-grams.
Finding the attributes of authorship is important to detect unique writing styles for each author. We
will build an attribute vector for each of the authors and determine the authorship based on the
similarity between pre-computed attribute vectors. We will use TF-IDF (Term Frequency Inverse
Document Frequency) to calculate the weights of each of the entities in the attribute vector.

1.1 Term Frequency
Suppose we have a collection of documents written by M authors. This collection of documents may
contain multiple books written by an author. Let’s define a set of documents written by same author as
a sub-collection j. We define fij to be the frequency (Number of occurrences) of term (word) i in subcollection
j.
TFij=0.5+0.5(fij/maxk*fkj)
In this assignment, we use the augmented TF to prevent a bias towards longer documents. E.g. raw
frequency divided by the maximum raw frequency of any term k in the sub-collection j. During this
process, you should not eliminate stop words. The most frequent term in the sub-collection will have a augmented TF value of 1.

1.2 Inverted Document Frequency
Suppose that term i appears in ni sub-collections within the corpus. For this assignment, we define the
IDFI, as:
IDFi = log10(N/ni)
where, N is the total number of sub-collections (number of authors).

1.3 TF.IDF value
The TF-IDF score is defined as TFij x IDFi
. The terms with the highest TF-IDF score are considered the
best words that characterize the document.

1.4 Calculating Cosine Distance
The result of performing calculations outlined in sections 1.1 through 1.3 is that each set of books
written by the same authror in your corpus will have an Author Attribute Vector (AAV),
AAVm = (TF.IDFword1, TF.IDFword2, TF.IDFword3, TF.IDFword4, ….., TF.IDFm)
Every author will have his or her own AAV representing writing style. To compare between an arbitrary
AAV (from the document with unknown authorship) and existing AAVs, all of the AAVs must have the
same dimension. The AAVs will be used to calculate the Cosine Distance to measure the similarity
between the authors’ writing styles. Suppose that we have two authors with vectors,
AAV1 = [x1, x2,	…xm] and AAV2 =[ y1, y2, …, ym]. The Cosine Similarity between them is defined as,
CosSimilarity = cos(θ) = (A•B)/(|| A |||| B ||)
Finally, your system should be able to provide a ranked list of authors (top 10 most similar authors) for
a document with unknown authorship.	

Software Requirements
Your software should perform two sets of functionalities: off-line computing and command-line
software.

2.1 Off-Line computing
This phase includes multiple steps;

(1) You should calculate the TF, IDF, and TF-IDF values for all terms for all of the sub-collections in
your corpus. You are required to use MapReduce(s) for this step. Custom implementations
without using MapReduce is disallowed.

(2) You should create the AAVs (author attribute vectors) for every author.

(3) You should store the results (author attribute vectors) in a HDFS file.

2.2 Command-Line software

(1) Read a document with unknown authorship and create an attribute vector for it. Make sure the
dimensionality of this vector should be identical to the one used in the off-line computing
(section 2.1).

(2) You should calculate the Cosine Similarity between the author attribute vector for this
document and all of the author attribute vectors calculated in the section 2.1, and select top 10
authors. 	
