Initial One Time Set Up Tasks

1. Converting Image to Feature Vector (Just the Histogram, i.e. RGB intensity values).
2. Format the Name node create directory based upon the various type of MI types expected.
3. Initialize directory with text file containing the histogram of a respective MI for the directory.
4. Get all the MI images to add to the dfs directory, iterate over each of them to generate histogram and compare (Euclidian distance) it to the histogram values stored in the text file against each MI type directory.
5. Put the respective file to the directory with the least Euclidian distance.
