**Initial One Time Set Up Tasks**

1. Add the various types of classification to be supported by the Hadoop Image Manager under key HimageManager.Classifications as CSVs.
2. Give your Hadoop Working directory under HimageManager.HadoopDirectory.
3. As code now only supports fixed dimension images as comparision works only on simple RGB intensity vectors, provide the dimension under HimageManager.ImageHeight and HimageManager.ImageWidth.
4. Provide your URI to your hadoop server under HimageManager.HadoopURI.

**How to run**

1. Generate a jar (optional you could use the jar file from the repo. itself)
2. Execute the jar through 
```
hadoop jar <jar name>
```
NOTE : config file should be present in the same directory as from where jar file is being executed.
