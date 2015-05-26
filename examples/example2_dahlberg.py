import luigi
import luigipp

# ------------------------------------------------------------------------
# Task classes
# ------------------------------------------------------------------------

#Rsync a folder
class RSyncAFolder(luigipp.LuigiPPTask):
    pass

#Run a program that takes 10 minutes to run
class Run10MinuteSleep(luigipp.LuigiPPTask):
    pass

#Perform a web request
class DoWebRequest(luigipp.LuigiPPTask):
    pass

#If it succeeds, continue

#If it fails, die

#Split a file
class SplitAFile(luigipp.LuigiPPTask):
    pass

#Run the same program on both parts of the split
class DoSomething(luigipp.LuigiPPTask):
    pass

#Merge the results of the programs
class MergeFiles(luigipp.LuigiPPTask):
    pass

