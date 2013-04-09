#!/usr/bin/perl -w

use strict;

use threads;
use Thread::Queue;
use Getopt::Long;
use Pod::Usage;

# Parametres du script
# Les requetes doivent etres separees par un
# Le parseur est basique : on considère qu'une requete est finie quand ; puis retour a la ligne
my $requetes;



# Variables globales

# La dataqueue. C'est une file d'attente entre threads, qui permet de passer des messages.
# Elle va servir a soumettre la liste de requetes lues au pool de threads les executant.
my $dataqueue = Thread::Queue->new;

# La liste des threads. Ne sert qu'a les attendre a la fin du programme, afin de ne s'arreter
# que quand tous les threads ont fini
my @threads;
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
# Elle prend toutes les requetes et les envoie dans la dataqueue
# Elle pousse aussi un message 'EXIT' par thread, afin de leur demander de s'arreter d'eux memes quand ils ont
# fini leur traitement
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
			$dataqueue->enqueue($requete);
			$requete='';
		}
	}
	# On envoie autant de messages d'arret que de threads.
	for (my $i=0;$i<$conf{'nb_req_paralleles'};$i++)
	{
		$dataqueue->enqueue('EXIT');
	}
	close FIC;
}

# Fonction worker : cete fonction recoit un message de la file d'attente et le traite (execute la requete).
# Elle sort quand elle recoit un message 'EXIT'
# A l'initialisation, on monte une session a la base.
sub worker
{
	# Creation de la session du thread a la base
	my $dbh;
	while (my $requete=$dataqueue->dequeue())
	{
		if ($requete eq 'EXIT')
		{
			threads->exit();
		}
		open ($dbh,"| psql -e");
#		print "$requete\n";
		print $dbh $requete;
		close $dbh;
	}
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

# On demarre les threads, on fait la lecture des requetes, on attend la mort des threads
for (my $i=0;$i<$conf{'nb_req_paralleles'};$i++)
{
	my $thread = threads->new(\&worker) or die "Impossible de creer un thread\n";
	push @threads,$thread;
}


foreach my $thread (@threads)
{
	$thread->join();
}
