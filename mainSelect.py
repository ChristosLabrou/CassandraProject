import Erotima1
import Erotima2
import Erotima4
import Erotima3
import Erotima5
import AstraConnect

def Erotima2Selection():
    titlesList = ['Jumanji'] + Erotima2.RandomizeFullDetailsQueries(9)
    Erotima2.SelectFullDetails(AstraConnect.QuorumProfile, titlesList)
    Erotima2.SelectFullDetails(AstraConnect.AllProfile, titlesList)
    Erotima2.SelectFullDetails(AstraConnect.OneProfile, titlesList)

def Erotima3Selection():
    genresList = ['Adventure', 'Comedy', 'Documentary', 'Drama', 'Romance', 'Action', 'Crime', 'Horror', 'Sci-Fi', 'Western']
    file = open("Erotima3.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.QuorumProfile) + " times:\n")
    file.close()
    for i in range(0, len(genresList)):
        Erotima3.SelectGenreByYear(AstraConnect.QuorumProfile, genresList[i])
    file = open("Erotima3.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.AllProfile) + " times:\n")
    file.close()
    for i in range(0, len(genresList)):
        Erotima3.SelectGenreByYear(AstraConnect.AllProfile, genresList[i])
    file = open("Erotima3.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.OneProfile) + " times:\n")
    file.close()
    for i in range(0, len(genresList)):
        Erotima3.SelectGenreByYear(AstraConnect.OneProfile, genresList[i])

def Erotima5Selection():
    tagsList = ['comedy', 'boring', 'war', 'family', 'sci-fi', 'light', 'animation', 'dark', 'classic', 'zombies']
    file = open("Erotima5.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.QuorumProfile) + " times:\n")
    file.close()
    for i in range(0, len(tagsList)):
        Erotima5.SelectTags(AstraConnect.QuorumProfile, tagsList[i])
    file = open("Erotima5.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.AllProfile) + " times:\n")
    file.close()
    for i in range(0, len(tagsList)):
        Erotima5.SelectTags(AstraConnect.AllProfile, tagsList[i])
    file = open("Erotima5.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.OneProfile) + " times:\n")
    file.close()
    for i in range(0, len(tagsList)):
        Erotima5.SelectTags(AstraConnect.OneProfile, tagsList[i])

def Erotima4Selection():
    wordsList = ['star', 'friend', 'war', 'family', 'toy', 'light', 'dragon', 'knight', 'opera', 'world']
    file = open("Erotima4.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.QuorumProfile) + " times:\n")
    file.close()
    for i in range(0, len(wordsList)):
        Erotima4.SelectTitle(AstraConnect.QuorumProfile, wordsList[i])
    file = open("Erotima4.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.AllProfile) + " times:\n")
    file.close()
    for i in range(0, len(wordsList)):
        Erotima4.SelectTitle(AstraConnect.AllProfile, wordsList[i])
    file = open("Erotima4.txt", "a")
    file.write(AstraConnect.ProfileToString(AstraConnect.OneProfile) + " times:\n")
    file.close()
    for i in range(0, len(wordsList)):
        Erotima4.SelectTitle(AstraConnect.OneProfile, wordsList[i])



Erotima2Selection()
Erotima3Selection()
Erotima4Selection()
Erotima5Selection()