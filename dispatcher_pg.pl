#!/usr/bin/perl -w

use strict;

use Getopt::Long;
use Pod::Usage;

# Parametres du script
# Les requetes doivent etres separees par un
# Le parseur est basique : on considère qu'une requete est finie quand ; puis retour a la ligne
my $requetes;



# Variables globales

# La liste des threads. Ne sert qu'a les attendre a la fin du programme, afin de ne s'arreter
# que quand tous les threads ont fini
my %sons;
# Le hash de configuration
my %conf;


# Cette fonction remplit le hash global %conf. C'est un parseur simple du fichier de configuration :
# Tous les parametres doivent etre au format cle = valeur (blancs optionnels)
sub charge_conf
{
	my ($fic_conf)=@_;
	open CONF,$fic_conf or die "Impossible d'ouvrir $fic_conf\n";
	while (my $ligne = <CONF>)
	{
		$ligne =~ /^(\S+)\s*=\s*(\S.*)/ or die "Impossible de parser la ligne $ligne de $fic_conf\n";
		$conf{$1}=$2;
	}
	close CONF;

	# Verifions que tous les parametres dont j'ai besoin sont la
	foreach my $cle ('host','port','user','passwd','database','nb_req_paralleles')
	{
		if (not defined $conf{$cle})
		{
			die "$cle n'est pas defini dans le fichier de configuration";
		}
	}
}



# Fonction reader : cette fonction lit le fichier de requetes. Elle est dans la boucle principale, pas dans un thread
# Elle prend toutes les requetes et les exécute
sub reader
{
	my $requete='';
	open FIC,$requetes or die "Impossible d'ouvrir $requetes\n";
	while (my $ligne = <FIC>)
	{
		$requete.=$ligne;
		if ($ligne =~ /;\s*$/)
		{
			# On vient de finir la requete. On supprime le ';'
			$requete=~ s/;\s*$//;
			runquery($requete);
			$requete='';
		}
	}
	close FIC;
}


# Fonction runquery: cette fonction reçoit une requête à exécuter. Elle l'exécute dès qu'un fils est disponible
# Elle fait donc:
# wait d'un fils si le max est atteint. Le fils appelle worker avec la requête. Le pere rajoute le fils dans le tableau
# fork d'un fils tant qu'on est sous le max
sub runquery
{
	my ($query)=@_;
	if (scalar(keys(%sons))==$conf{nb_req_paralleles})
	{
		# On attend
		my $dead_son=wait();
		delete $sons{$dead_son};
	}
	# Ok, we have a slot
	my $son=fork();
	if (not $son)
	{
		worker($query);
	}
	else
	{
		$sons{$son}=1;
	}
}


# Fonction worker : cete fonction recoit une requête à exécuter
# A l'initialisation, on monte une session a la base.
sub worker
{
	my ($query)=@_;
	my $dbh;
	open ($dbh,"| psql -e");
	print $dbh $query;
	close $dbh;
}


# Main : on commence par parser la ligne de commande et le fichier de conf
my $fic_conf;
my $help;
my $getopt = GetOptions("conf=s" => \$fic_conf,
			"requetes=s" => \$requetes,
			"help" =>\$help);
if ($help or (not $fic_conf) or not ($requetes))
{
	Pod::Usage::pod2usage(-exitval => 1, -verbose => 3);
}


charge_conf($fic_conf);

# On remplit les files d'attente ...
reader();

# Pas besoin de section critique: les parametres de connexion sont les mêmes pour les 3 sessions
# Les variables d'environnement sont positionnées avant de démarrer les threads
$ENV{PGUSER}=$conf{'user'};
$ENV{PGPASSWORD}=$conf{'passwd'};
$ENV{PGPORT}=$conf{'port'};
$ENV{PGHOST}=$conf{'host'};
$ENV{PGDATABASE}=$conf{'database'};

# On attend la mort de tous les fils
do
{
} until (wait() == -1);
