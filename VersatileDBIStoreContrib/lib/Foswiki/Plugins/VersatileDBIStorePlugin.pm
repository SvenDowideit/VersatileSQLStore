# See bottom of file for default license and copyright information

package Foswiki::Plugins::VersatileDBIStorePlugin;
use strict;

use Foswiki();
use Foswiki::Store::Versatile ();

require Foswiki::Func;    # The plugins API
require Foswiki::Plugins; # For the API version

our $VERSION = '$Rev$';
our $RELEASE = '$Date: 2012-04-18 18:20:00 +0200 (Tue, 18 Apr 2012) $';
our $SHORTDESCRIPTION = 'Plugin to support the VersatileDBIStoreContrib';

our $NO_PREFS_IN_TOPIC = 1;

sub initPlugin {
    my( $topic, $web, $user, $installWeb ) = @_;

    if( $Foswiki::Plugins::VERSION < 2.0 ) {
        Foswiki::Func::writeWarning( 'Version mismatch between ',
                                     __PACKAGE__, ' and Plugins.pm' );
        return 0;
    }

    Foswiki::Func::registerRESTHandler('connect', \&restCONNECT);       # Test connection to configured database
    Foswiki::Func::registerRESTHandler('dbi', \&restDBI);               # Report DBI stuff
    Foswiki::Func::registerRESTHandler('upload', \&restUPLOAD);         # recreate tables and spin thru existing topics and populate DB
    
    Foswiki::Func::registerRESTHandler('wf', \&restWF);                 # view actual Foswiki::Func and Foswiki::Meta returned when reading specific topics (often created for testing)

    return 1;
}

use Foswiki::Prefs::TopicRAM ();
use Foswiki::Meta ();

sub restCONNECT {
    my $store = Foswiki::Store::Versatile->new();
    return $store->_errstr() . "\n\n";
}

sub restDBI {
    use DBI;

    my $text;
    my @driver_names = DBI->available_drivers;
  
    $text .= "ADN: @driver_names\n";

    my @data_sources = DBI->data_sources("mysql");
    for my $ds (@data_sources) {
        $text .= "DSN: $ds\n";
    }

    my %drivers = DBI->installed_drivers;
    $text .= keys %drivers;
    $text .= "\n";

    for my $d (keys %drivers) {
        $text .= "$d --> $drivers{$d}\n";
    }
    return "$text\n\n";
}

sub restUPLOAD {
    my ($session) = @_;
   
    my $store = Foswiki::Store::Versatile->new();
    my $text = $store->_recreateTables();

    my @webs = Foswiki::Func::getListOfWebs();
    my $usersWeb = $store->_insertWeb($Foswiki::cfg{UsersWebName});
        
    for my $w (@webs) {
        my @topics = Foswiki::Func::getTopicList($w);
        my $webId = $store->_insertWeb($w);
        for my $t (@topics) {
            my $topicId = $store->_insertTopic($webId,$t);
#            $store->_insertFlush();

            my ($meta, $topicText) = Foswiki::Func::readTopic( $w, $t );
            print STDERR ("$t: ". $meta->getLoadedRev() . "\n");
            #use Data::Dumper;
            #print STDERR Data::Dumper->Dump([$meta], [qw(meta)]) if $t eq 'GroupTemplate';
            my $prefs = Foswiki::Prefs::TopicRAM->new($meta);

            my @preflist = $prefs->prefs();
            for my $p (@preflist) {
                my $value = $prefs->{values}->{$p};
                if( $p =~ m{^(ALLOW|DENY)(ROOT|WEB|TOPIC)([A-Z]+)$} ) {
                    my $fid = $store->_insertField('_PREFERENCE',$p,'ACL');
                    $store->_insertValue($topicId,$fid,$value,$webId);
                    my ($ad, $rwt, $mode) = ($1, $2, $3);
                    # Dump the users web specifier if userweb
                    my @list = grep { /\S/ } map {
                        s/^($Foswiki::cfg{UsersWebName}|%USERSWEB%|%MAINWEB%)\.//;
                        $_
                    } split( /[,\s]+/, $value );
                    for my $access_id (@list) {
                        my $accessFobId = $store->_insertTopic($usersWeb,$access_id);
                        $store->_insertAccess($topicId, $accessFobId, $ad, $rwt, $mode);
                        # print STDERR "$w.$t pref=$ad - $rwt - $mode cUID=$access_id\n";
                    }
                }
                else {
                    my $fid = $store->_insertField('_PREFERENCE',$p,'SET');
                    $store->_insertValue($topicId,$fid,$value,$webId);
                }
            }
            my @locallist = $prefs->localPrefs();
            for my $p (@locallist) {
                my $fid = $store->_insertField('_PREFERENCE',$p,'LOCAL');
                $store->_insertValue($topicId,$fid,$prefs->{local}->{$p},$webId);
            }
            
            $store->_insertValue($topicId, $store->_fieldId('_text','',''), $topicText, $webId);
 
            my @metatypes = keys %$meta;
            TYPE:
            for my $metatype (@metatypes) {
                next TYPE if $metatype =~ /_.*?/;
                
                my @metanames = $meta->find($metatype);

                if($metatype eq 'FILEATTACHMENT') {
                    my $nameSeq = 0;       
                    for my $metanameRef (@metanames) {
                        my $metaname = $metanameRef->{name};
                        my $attachId = $store->_insertTopic($webId,'');
                        
                        my $h = sprintf("%08X %016X",$nameSeq++,$attachId);
                        my $attachXREF = $store->_insertField($metatype,undef,'*attach_xref');
                        $store->_insertValue($topicId,$attachXREF,$h,$webId);
                        $store->_insertXref($store->_insertField('_attachment','',''),$topicId,0,$attachId,0);
        
                        my @metakeys = keys %$metanameRef;
                        METAKEY1:
                        for my $metakey (@metakeys) {
                            next METAKEY1 unless $metakey =~ /^[A-Za-z0-9_]+$/; # SMELL: Foswiki::Meta should do a better job!
                            my $fid = $store->_insertField($metatype,undef,$metakey);
                            $store->_insertValue($attachId,$fid,$metanameRef->{$metakey},$webId);
                        }
                    }
#                    $store->_insertFlush();
                    next TYPE;
                }

                my $v = $Foswiki::Meta::VALIDATE{$metatype};
                if($v && !$v->{many}) {
                    my $metanameRef = $metanames[0];
                    
                    my @metakeys = keys %$metanameRef;
                    METAKEY2:
                    for my $metakey (@metakeys) {
                        next METAKEY2 unless $metakey =~ /^[A-Za-z0-9_]+$/; # SMELL: Foswiki::Meta should do a better job!
                        my $fid = $store->_insertField($metatype,$metanameRef->{name} ? '' : undef,$metakey);
                        $store->_insertValue($topicId,$fid,$metanameRef->{$metakey},$webId);
                    }
                    next TYPE;
                }

                my $nameSeq = 0;       
                
                for my $metanameRef (@metanames) {
                    my $metaname = $metanameRef->{name};
                    
                    my $h = sprintf("%08X",$nameSeq++);
                    # if($metaname) {
                        my $nameSeqId = $store->_insertField($metatype,$metaname,'name'); # Always $metanames[0]
                        $store->_insertValue($topicId,$nameSeqId,$h,$webId);
                    # }

                    my @metakeys = keys %$metanameRef;
                    METAKEY3:
                    for my $metakey (@metakeys) {
                        next METAKEY3 if $metakey eq 'name';
                        next METAKEY3 unless $metakey =~ /^[A-Za-z0-9_]+$/; # SMELL: Foswiki::Meta should do a better job!
                        my $fid = $store->_insertField($metatype,$metaname,$metakey);
                        $store->_insertValue($topicId,$fid,$metanameRef->{$metakey},$webId);
                    }
                }
            }
#            $store->_insertFlush();
        }
    }
    $store->_insertFlush(1);
    
    my $dbh = $store->_getDBH();
    
    use Time::HiRes qw(gettimeofday tv_interval);
    
    my $t = [gettimeofday];
    
    my $sth = $dbh->prepare("select * from values_string where fobid > 100 and fobid <= 120 limit 20000");
    $sth->execute();
    while(my @row = $sth->fetchrow_array) {
        my ($fobid, $fieldId, $ducktype, $value) = @row;
#        print STDERR "$row[0]; $row[1]; $row[2]; $row[3]\n";
    }
    
    print STDERR ("That took:" . tv_interval($t, [gettimeofday]) . " seconds\n");
    
    $t = [gettimeofday];
    
#    my $sth = $dbh->prepare("select * from values_string limit 20000");
    $sth->execute();
    while(my @row = $sth->fetchrow_array) {
        my ($fobid, $fieldId, $ducktype, $value) = @row;
#        print STDERR "$row[0]; $row[1]; $row[2]; $row[3]\n";
    }
    
    print STDERR ("That took:" . tv_interval($t, [gettimeofday]) . " seconds\n");
    
    
    return $text;
}

# See how a real topic (WorkFlow.txt hence WF) is returned. Topic created as a text file directly to force unusual situations
sub restWF {
    my ($session) = @_;
    my $query = $session->{request};
    my $w = $query->{param}->{w}[0] || 'Main';
    my $t = $query->{param}->{t}[0] || 'WorkFlow3.txt';

   my $text = '';
   
    my $oText = Foswiki::Func::readFile("$Foswiki::cfg{DataDir}/$w/$t.txt");
    my ($meta, $topicText) = Foswiki::Func::readTopic( $w, $t );

    $text .= "\n\n== $t" . "=" x 120 . "\n";
    $text .= $oText;
    $text .= "--------------------------------------------\n";
    $meta->{_text} = "*Text was 'ere*";
    $text .= $meta->getEmbeddedStoreForm();
    $text .= "--------------------------------------------\n";
    $text .= $topicText;
    $text .= "\n--------------------------------------------\n";
    my $column;
    my @types = keys %$meta;
    TYPE:
    for my $type (@types) {
        if($type =~ /_.*?/) {
            next;
            $text .= "$type = '";
            $text .= $meta->{$type} . "'\n";
            next;
        }
        my @items = $meta->find($type);
        if(scalar (@items) == 0) {
            $text .= "$type has no entries\n";
            next;
        }
        
        my $q = 0;
        for my $i (@items) {
            my @keys = keys %$i;
            $text .= '%META:' . "$type\[$q]{";
            $q += 1;
            my $ktext = '';
            for my $k (sort @keys) {
                if($k eq 'name') {
                    $text .= "$k='$i->{$k}' ";
                }
                else {
                    $ktext .= "$k='$i->{$k}' ";
                }
            }
            $text .= "$ktext}\n";
        }
    }
    $text .= "\n\n";
   
    return $text;
}

1;
__END__
This copyright information applies to the VersatileDBIStorePlugin

Foswiki - The Free and Open Source Wiki, http://foswiki.org/

Copyright (C) 2012 Julian Levens

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.
