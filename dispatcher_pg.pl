#!/usr/bin/perl -w

use strict;

use Getopt::Long;
use Pod::Usage;

# Sorry this is not translated yet
# Just pass -conf to point to the configuration file and -query to the query file
# Parametres du script
# Les queries doivent etres separees par un
# Le parseur est basique : on considère qu'une query est finie quand ; puis retour a la ligne
my $queries;



# Variables globales

# La liste des fils. Ne sert qu'a les attendre a la fin du programme, afin de ne s'arreter
# que quand tous les fils ont fini
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
	foreach my $cle ('host','port','user','passwd','database','nb_parallel_queries')
	{
		if (not defined $conf{$cle})
		{
			die "$cle n'est pas defini dans le fichier de configuration";
		}
	}
}



# Fonction reader : cette fonction lit le fichier de queries. Elle est dans la boucle principale
# Elle prend toutes les queries et les exécute
sub reader
{
	my $query='';
	open FIC,$queries or die "Impossible d'ouvrir $queries\n";
	while (my $ligne = <FIC>)
	{
		$query.=$ligne;
		if ($ligne =~ /;\s*$/)
		{
			# On vient de finir la query. On supprime le ';'
			$query=~ s/;\s*$//;
			runquery($query);
			$query='';
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
	if (scalar(keys(%sons))==$conf{nb_parallel_queries})
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
	exit 0;
}


# Main : on commence par parser la ligne de commande et le fichier de conf
my $fic_conf;
my $help;
my $getopt = GetOptions("conf=s" => \$fic_conf,
			"queries=s" => \$queries,
			"help" =>\$help);
if ($help or (not $fic_conf) or not ($queries))
{
	Pod::Usage::pod2usage(-exitval => 1, -verbose => 3);
}


charge_conf($fic_conf);


# Les variables d'environnement sont positionnées avant de démarrer le reste. C'est toujours ça qui ne sera plus à faire
$ENV{PGUSER}=$conf{'user'};
$ENV{PGPASSWORD}=$conf{'passwd'};
$ENV{PGPORT}=$conf{'port'};
$ENV{PGHOST}=$conf{'host'};
$ENV{PGDATABASE}=$conf{'database'};

# On remplit les files d'attente ...
reader();


# On attend la mort de tous les fils
do
{
} until (wait() == -1);
