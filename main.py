import Erotima1
import Erotima2
import Erotima4
import Erotima3
import Erotima5
import AstraConnect

i = 1
file = open("TimeComparison.txt", 'a')
file.write("\nErotima %d\n"%i)
file.close()
i = i+1
#Erotima 1
Erotima1.InsertRatings(AstraConnect.QuorumProfile, True)
Erotima1.InsertRatings(AstraConnect.AllProfile, True)
Erotima1.InsertRatings(AstraConnect.TwoProfile, True)

file = open("\nTimeComparison.txt", 'a')
file.write("\nErotima %d\n"%i)
file.close()
i = i+1
#Erotima 2
Erotima2.InsertFullDetails(AstraConnect.QuorumProfile, True)
Erotima2.InsertFullDetails(AstraConnect.AllProfile, True)
Erotima2.InsertFullDetails(AstraConnect.TwoProfile, True)

file = open("\nTimeComparison.txt", 'a')
file.write("\nErotima %d\n"%i)
file.close()
i = i+1
#Erotima 3
Erotima3.InsertGenreByYear(AstraConnect.QuorumProfile, True)
Erotima3.InsertGenreByYear(AstraConnect.AllProfile, True)
Erotima3.InsertGenreByYear(AstraConnect.TwoProfile, True)

file = open("\nTimeComparison.txt", 'a')
file.write("\nErotima %d\n"%i)
file.close()
i = i+1
#Erotima 4
Erotima4.InsertTitle(AstraConnect.QuorumProfile, True)
Erotima4.InsertTitle(AstraConnect.AllProfile, True)
Erotima4.InsertTitle(AstraConnect.TwoProfile, True)

file = open("\nTimeComparison.txt", 'a')
file.write("\nErotima %d\n"%i)
file.close()
i = i+1
#Erotima 5
Erotima5.InsertTags(AstraConnect.QuorumProfile, True)
Erotima5.InsertTags(AstraConnect.AllProfile, True)
Erotima5.InsertTags(AstraConnect.TwoProfile, True)