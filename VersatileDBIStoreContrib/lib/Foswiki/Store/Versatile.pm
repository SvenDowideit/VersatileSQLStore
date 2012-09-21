# See bottom of file for license and copyright information

=begin TML

---+ package Foswiki::Store::Versatile

=cut

package Foswiki::Store::Versatile;
use strict;
use warnings;

#use File::Copy            ();
#use File::Copy::Recursive ();
use Fcntl qw( :DEFAULT :flock );

use Foswiki::Store ();
our @ISA = ('Foswiki::Store');

use Assert;
use Error qw( :try );

use Foswiki                                ();
use Foswiki::Meta                          ();
use Foswiki::Sandbox                       ();
use Foswiki::Iterator::NumberRangeIterator ();
use Foswiki::Users::BaseUserMapping        ();

BEGIN {  # Don't think I need this. Won't Sort be delegated to the DB?
    # Import the locale for sorting
    if ( $Foswiki::cfg{UseLocale} ) {
        require locale;
        import locale();
    }
}

sub _recreate {
    my ($dbh, $table, $columns, $engine) = @_;

    my $sth = $dbh->do("drop table if exists $table");    
    return "drop $table failed=(" . $DBI::err . "::". $DBI::errstr . ")\n" if $dbh->err;
    
    my $text .= "$table drop OK;";

    $engine = '' if !defined $engine;
    $engine = ", engine = $engine" if $engine;
    $sth = $dbh->do("create table $table ($columns) character set utf8mb4, collate utf8mb4_bin$engine");
    return $text . " create failed=(" . $DBI::err . "::". $DBI::errstr . ")\n" if $dbh->err;
    
    return $text . " create OK\n";
}

sub _reindex { # Dropping the table will drop the index
    my ($dbh, $type, $index, $table, $columns) = @_;

    my $sth = $dbh->do("create $type index $index on $table ($columns)");
    return "Create index failed=(" . $DBI::err . "::". $DBI::errstr . ")\n" if $dbh->err;
    
    return "Create index $index OK\n";
}
# 
# To find correct ip address I used ping -4 julianmark-pc to get the ipv4 version, then place into the config var
# You will need a mysql client (not full fat) on linux to establish the connection
# Sql branch of vagrant installs various mysql libraries, I added mysql-client
# However,  error was "Can't connect to local MySQL server through socket '/var/run/mysqld/mysqld.sock'"

# Eventually I installed mysql server as well - but that was not an immediate solution, possibly not required at all: still I might test this for comparative speed
# 
# Realised that the ip address above was wrong
# Used telnet to test connection: telnet 192.168.0.4 3306
# The 3306 port number is important as it's mysql's port number
# Once you make the telnet connection you have to escape with '^]' followed by quit, but now the connection is proven
# 
# The VM seemed to have lost the resolver, so I could not apt-get install to work (fixed by halt then up)
# 
# With the right ip it was easy (but was that the only problem?)

sub new {
    my $class = shift;
    my $this  = $class->SUPER::new(@_);
    unless ( $this->{connected} ) {
        $this->{connected} = 1;

        # my $dbh = DBI->connect("DBI:mysql:foswiki:192.168.0.4","vagrant","m2Vagrant_");
        # Carefully set-up in LocalSite.cfg when building a new VM
        #     (three distinct hash entries with there own name and value!!)
        #
        # Everything else is good to go on the VM sql wise
        #    It's also possible to mysql -h 192.168.0.4 -u foswiki -p and connect that way (nothing else to configure)
        #
        my $con = $Foswiki::cfg{VersatileDBIStore}{connection};
        my $dbu = $Foswiki::cfg{VersatileDBIStore}{dbuser};
        my $dbp = $Foswiki::cfg{VersatileDBIStore}{dbpass};
        
        my $dbh = DBI->connect($con,$dbu,$dbp); #SMELL Exception handling would be good!
        $this->{dbh} = $dbh;
        if($dbh->err) {
            $this->{errstr} = "Connect to $con user $dbu failed (err=" . $dbh->err . "; msg=" . $dbh->errstr . ")";
        }
        else {
            $this->{errstr} = "Connect OK";
            $this->{webExistsSt} = $dbh->prepare("select count(*) from web where name = ?");
            $this->{webIdSt} = $dbh->prepare("select fobid from FOB where webid = 0 AND name = ?");
            $this->{topicIdSt} = $dbh->prepare("select fobid from FOB where webid = ? AND name = ?");
            $this->{fieldIdSt} = $dbh->prepare("select id from fields where metatype = ? AND hasMetamember = ? AND metamember = ? AND metakey = ?");
            $this->{groupIdSt} = $dbh->prepare("select id from groups where type = ? AND name = ?");
            $this->{webs} = {};
            $this->{fields} = {};
            $this->{groups} = {};
            $this->{topics} = {};
            $this->{values_string} = [];
            $this->{values_string_v} = [];
            $this->{values_double} = [];
            $this->{values_double_v} = [];
            $this->{values_datetime} = [];
            $this->{values_datetime_v} = [];
            $this->{done_it} = 0;

            $this->{Number} = qr/^\s*?[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?\s*?$/o;
        } 
    }

    return $this;
}

sub _errstr {
    my $this = shift;
    return $this->{errstr};
}

sub _recreateTables {
    my $this = shift;   
    my $dbh = $this->{dbh};
    my $text;
    
    $text .= _recreate($dbh,"fobID","fobid bigint unsigned auto_increment primary key");
    # $text .= _recreate($dbh,"web","id mediumint unsigned auto_increment unique key, name varchar(190), constraint primary key (name)");
    
    # The length of name can be increased to 188, by leaving at 180 I have 30 bytes available for the future
    # The type is fob type is one of:
    #
    #      1: Root (and webid = 0)
    #      2: Web  (and webid = 0)
    #      3: Shadow Web (and webid = 0)
    #      4: Topic (and webid = fobid of a previously created Web)
    #      5: Shadow Topic (and webid = fobid of a previously created Web)
    # Also add 16 if it's a forward reference

    $text .= _recreate($dbh,"FOB","fobid bigint unsigned, webid bigint unsigned, name varchar(180), type tinyint unsigned, constraint primary key (type, webid, name)");
    
    # How did I solve the sequence problem?
    #   * That is to say that all META:TYPE repeated (made unique by name) in a topic should be returned as an array in Foswiki::Meta maintaining the order
    #   * Comes for free in text files, need to work at it for SQL
    #
    # Basic solution is that each META:TYPE{name= is assigned a values_string in hex (remember utf-8, hence cannot use binary actually could use 7-bit chars to be compact, but I need to be aware of utf8 collation order)
    #   * top 4 bytes are used for the main sequence: hence 4 billion entries (must start at one not zero, to allow room above)
    #   * extra characters can be added after this at any time to allow for the (future) possibility to insert new entries without resequencing as this will cause much versioning work
    #   * I've used values_string as that's where most data is kept as it provides good locality
    #   * It also serves to indicate the presence of the relavent META:TYPE{name even in the absence of any other keys and values, (and will still be ordered properly)
    #   * Remember META:TYPE{name is a unique field_id, therefore the value had no clear meaning, until now
    
    # The max key length in MySQL is 767 bytes which is only 191 utf8mb4 characters (767 / 4 = 191 chars remainder 3 bytes)
    #
    # META:VERSATILEDBISTORELONGMETANAME
    # META:123456789-123456789-123456789-
    #
    # Note that the primary key is not the field-id, this is to allow repeats to define aliases (but do we want them?)
    #
    # field.type is 1 for duck type (i.e. support the initial release) any other value is to extend fields either for new dataform field types or mapping to existing SQL tables
    # field.type is 2 for xref.reason
    # field.type is 3 are for fields mapped to other tables
    #
    # metamember contains the value of the name="..." part of META:TYPE{name="Abc" key1="x" key2="y" etc} OR
    #                                                         META:TYPE{name="" key1="x" key2="y"} OR
    #                                                         META:TYPE{key1="x" key2="y"}
    # In the last two cases Foswiki::Meta treats name as "" i.e they are the same unique key. Other metamember is set to "Abc" to form the unique key
    # 
    $text .= _recreate($dbh,"fields","id int not null auto_increment unique key, type tinyint not null default 1, metatype varchar(25), hasMetamember tinyint, metamember varchar(150), metakey varchar(15) not null, constraint field_pk primary key (metatype, hasMetamember, metamember, metakey)");
        
    # Modes in the following are specially created 'field-id's
    # access_id is a fobid which is also specifically one-of:
    #    * cUID
    #    * GroupTopic (a unique id for the group)
    #    * no-one (a logical Group)
    #    * any-one (a logical Group)
    #    * XxxWebDenyMODEGroup \ logically created one per Web and MODE combination
    #    * XxxWebAllowMODEGroup } which simply maps the list of cUIDs and groups in the WebPreference setting
    #    * XxxWebPermitMODEGroup / Despite being called permit, it's value can be 'D' for denied
    $text .= _recreate($dbh,"access","fobid bigint, access_id bigint, mode int, priority tinyint, permit tinyint, constraint primary key (fobid, access_id, mode, priority)");
#    $text .= _recreate($dbh,"groups","id int not null auto_increment, type tinyint, name varchar(190), constraint primary key (id, type, name)");
#    $text .= _recreate($dbh,"groupClosure","id int, member int, direct int, constraint primary key (id, member, direct)");
    
    # DuckType: 1=Number (& string), 2=Date (& string),  3=Date or number (& string), 4=String only
    # Will I need to consider an item like "5 pounds" as a number (from sorting/indexing point of view?). If so, is that a another duck-type: prefixed-number?
    # With ducktype before value then sorting will naturally be in separate (albeit sorted) blocks. Will the DB be smart enough to merge sort these blocks?
    # This can be fixed by adding another index which excludes ducktype, this will also speed up pure string searches
    #
    # A search term of item op 23.5, would first search values_double and select matches, then search values_string with ducktype in (2,4) i.e dates and strings (non-nums)
    # A search term of item op date(), would first search values_datetime and select matches, then search values_string with ducktype in (1,4) i.e nums and strings (non-dates)
    #
    # Foswiki::Time::parseTime accepts any integer >= 60 as a year and therefore all of these will be seen as both date and number. However,
    # this is probably rather aggressive for most sites. Therefore a config option to say only treat integers in range xxx..yyyy as years and hence dates
    # for indexing purposes a range of 0..0 to never treat integers as a possible date, hence no field will be seen as a date and number at the same time.
    
    # Are 21000 (utf8) characters enough for any META field in FW? The largest I can think of are text-areas for dataforms but that's pretty big. I could
    # change this to a MEDIUMTEXT (about 5MB but that's stored separately hence slower - probably offset by 251 bytes being stored and retrieved directly in the index)
    
    $text .= _recreate($dbh,"values_string","fobid bigint, field_id int, ducktype tinyint, value mediumtext, constraint primary key (fobid, field_id, value(180))","MyISAM");
    $text .= _reindex($dbh,"","values_string_fieldNvalue","values_string","field_id, value(180)"); # NB not a unique index, same field & value possible in diff topics
    $text .= _reindex($dbh,"fulltext","values_string_fts","values_string","value");
    #my $str = "Julian Mark Levens" x 1166;
    #my $sth = $dbh->prepare("insert into values_string values(100,1,4,?);");
    #$sth->execute($str);
    
    # Used for Duck-indexing of data that looks like a number
    $text .= _recreate($dbh,"values_double","fobid bigint, field_id int, value double, constraint primary key (fobid, field_id, value)");
    $text .= _reindex($dbh,"","values_double_fieldNvalue","values_double","field_id, value"); # NB not a unique index, same field & value possible in diff topics
    
    # Used for Duck-indexing of data that looks like a date [time]
    $text .= _recreate($dbh,"values_datetime","fobid bigint, field_id int, value datetime, constraint primary key (fobid, field_id, value)");
    $text .= _reindex($dbh,"","values_datetime_fieldNvalue","values_datetime","field_id, value"); # NB not a unique index, same field & value possible in diff topics
    #
    # For future use (actually storing dates), please note:
    # 'YYYY-MM-DD HH:MM:SS' format for MySQL (and other flavours?) not really what we want. To support ISO8601 fully I'll need extra columns for MySQL, other SQL flavours
    # have timezone support
    # I'll also need to ensure I can index this properly (datetime at same point in time are index/sorted as such even if recorded with diff TZ)
    # MySQL does have a CONVERT_TZ function, not sure if this helps
    #
    # Conversely, FW only has loose date[time] types at the moment. Therefore for transitioning this may be OK.    
    
    # $text .= _recreate($dbh,"values_text","fobid bigint, field_id int, value text, constraint primary key (fobid, field_id, value(180))");
        
    # Tables yet to create
    #     name_table: with normal & reverse index
    #
    # Need to think clearly about primary indexes
    #                             secondary indexes - possibly unique
    #                             foreign keys to establish

    # field_id and xref_field can be 0 to indicate a reference just to a particular FOB rather than a particular field within a FOB
    # I wonder about the possibility of a xref_id (a la field table) to distinguish distinct types of xref. That would require another xref_id table and so on.
    # In practice I think that this is not necessary as the combination of field_id and xref_field will probably serve the same purpose. Indeed, if the combination
    # is duplicated what would be the benefit of providing a further distinction (no I am not clear on this idea).
    $text .= _recreate($dbh,"xref","fobid bigint, field_id int, xref_fobid bigint, xref_field_id int, reason int, constraint primary key (fobid, field_id, xref_fobid, xref_field_id, reason)");
    $text .= _reindex($dbh,"unique","xref_reverse","xref","xref_fobid, xref_field_id, fobid, field_id, reason");
    
    $text .= "\n\n";
    
    # Pre-define a few field-ids
    my $fid;
    $fid = $this->_insertField('_text','','');
    
    # Ideally scan through Foswiki::Meta::VALIDATE and pre-define the fields that we can


    # The standard access modes
    $fid = $this->_insertField('_access_mode',undef,'VIEW');
    $fid = $this->_insertField('_access_mode',undef,'CHANGE');
    $this->_insertField('_access_mode',undef,'RENAME');
}

sub finish {
    my $this = shift;
#    print "Finishing\n";
#    $this->_insertFlush(1); # Only relevant during upload why is this finish not called at program exit?
    my $sth = $this->{dbh}->disconnect if $this->{connected};
    undef $this->{connected};
    undef $this->{dbh};
    undef $this->{errstr};
    $this->SUPER::finish();
}

# Implement Foswiki::Store
sub readTopic {
    my ( $this, $meta, $version ) = @_;

    my ( $gotRev, $isLatest ) = $this->askListeners( $meta, $version );
    if ( defined($gotRev) && ( $gotRev > 0 || $isLatest ) ) {
        return ( $gotRev, $isLatest );
    }
    ASSERT( not $isLatest ) if DEBUG;
    $isLatest = 0;

    # check that the requested revision actually exists
    my $nr = _numRevisions($meta);
    if ( defined $version && $version =~ /^\d+$/ ) {
        $version = $nr if ( $version == 0 || $version > $nr );
    }
    else {
        undef $version;

        # if it's a non-numeric string, we need to return undef
        # "...$version is defined but refers to a version that does
        # not exist, then $rev is undef"
    }

    ( my $text, $isLatest ) = _getRevision( $meta, undef, $version );

    unless ( defined $text ) {
        ASSERT( not $isLatest ) if DEBUG;
        return ( undef, $isLatest );
    }

    $text =~ s/\r//g;    # Remove carriage returns
                         # Parse meta-data out of the text
    $meta->setEmbeddedStoreForm($text);

    $version = $isLatest ? $nr : $version;

    # Patch up the revision info
    $meta->setRevisionInfo(
        version => $version,
	date  => ( stat ( _latestFile($meta) ) )[9]
    );

    return ( $version, $isLatest );
}

# Implement Foswiki::Store
sub moveAttachment {
    my ( $this, $oldTopicObject, $oldAttachment, $newTopicObject,
        $newAttachment, $cUID )
      = @_;

    # No need to save damage; we're not looking inside

    my $oldLatest = _latestFile( $oldTopicObject, $oldAttachment );
    if ( -e $oldLatest ) {
        my $newLatest = _latestFile( $newTopicObject, $newAttachment );
        _moveFile( $oldLatest, $newLatest );
        _moveFile(
            _historyDir( $oldTopicObject, $oldAttachment ),
            _historyDir( $newTopicObject, $newAttachment )
        );

        $this->tellListeners(
            verb          => 'update',
            oldmeta       => $oldTopicObject,
            oldattachment => $oldAttachment,
            newmeta       => $newTopicObject,
            newattachment => $newAttachment
        );
        _recordChange( $oldTopicObject, $cUID, 0 );
    }
}

# Implement Foswiki::Store
sub copyAttachment {
    my ( $this, $oldTopicObject, $oldAttachment, $newTopicObject,
        $newAttachment, $cUID )
      = @_;

    # No need to save damage; we're not looking inside

    my $oldbase = _getPub($oldTopicObject);
    if ( -e "$oldbase/$oldAttachment" ) {
        my $newbase = _getPub($newTopicObject);
        _copyFile(
            _latestFile( $oldTopicObject, $oldAttachment ),
            _latestFile( $newTopicObject, $newAttachment )
        );
        _copyFile(
            _historyDir( $oldTopicObject, $oldAttachment ),
            _historyDir( $newTopicObject, $newAttachment )
        );

        $this->tellListeners(
            verb          => 'insert',
            newmeta       => $newTopicObject,
            newattachment => $newAttachment
        );
        _recordChange( $oldTopicObject, $cUID, 0 );
    }
}

# Implement Foswiki::Store
sub attachmentExists {
    my ( $this, $meta, $att ) = @_;

    # No need to save damage; we're not looking inside
    return -e _latestFile( $meta, $att )
      || -e _historyFile( $meta, $att );
}

# Implement Foswiki::Store
sub moveTopic {
    my ( $this, $oldTopicObject, $newTopicObject, $cUID ) = @_;

    _saveDamage($oldTopicObject);

    my $rev = _numRevisions($oldTopicObject);

    _moveFile( _latestFile($oldTopicObject), _latestFile($newTopicObject) );
    _moveFile( _historyDir($oldTopicObject), _historyDir($newTopicObject) );
    my $pub = _getPub($oldTopicObject);
    if ( -e $pub ) {
	_moveFile( $pub,     _getPub($newTopicObject) );
    }

    $this->tellListeners(
        verb    => 'update',
        oldmeta => $oldTopicObject,
        newmeta => $newTopicObject
    );

    if ( $newTopicObject->web ne $oldTopicObject->web ) {

        # Record that it was moved away
        _recordChange( $oldTopicObject, $cUID, $rev );
    }

    _recordChange( $newTopicObject, $cUID, $rev );
}

# Implement Foswiki::Store
sub moveWeb {
    my ( $this, $oldWebObject, $newWebObject, $cUID ) = @_;

    # No need to save damage; we're not looking inside

    my $oldbase = _getData($oldWebObject);
    my $newbase = _getData($newWebObject);

    _moveFile( $oldbase, $newbase );

    $oldbase = _getPub($oldWebObject);
    if (-e $oldbase) {
	$newbase = _getPub($newWebObject);

	_moveFile( $oldbase, $newbase );
    }

    $this->tellListeners(
        verb    => 'update',
        oldmeta => $oldWebObject,
        newmeta => $newWebObject
    );

    # We have to log in the new web, otherwise we would re-create the dir with
    # a useless .changes. See Item9278
    _recordChange( $newWebObject, $cUID, 0,
        'Moved from ' . $oldWebObject->web );
}

# Implement Foswiki::Store
sub testAttachment {
    my ( $this, $meta, $attachment, $test ) = @_;
    my $fn = _latestFile( $meta, $attachment );
    return eval "-$test '$fn'";
}

# Implement Foswiki::Store
sub openAttachment {
    my ( $this, $meta, $att, $mode, @opts ) = @_;
    return _openStream( $meta, $att, $mode, @opts );
}

# Implement Foswiki::Store
sub getRevisionHistory {
    my ( $this, $meta, $attachment ) = @_;

    my $itr = $this->askListenersRevisionHistory( $meta, $attachment );
    return $itr if defined($itr);

    unless ( -e _historyDir( $meta, $attachment ) ) {
        my @list = ();
        require Foswiki::ListIterator;
        if ( -e _latestFile( $meta, $attachment ) ) {
            push( @list, 1 );
        }
        return Foswiki::ListIterator->new( \@list );
    }

    return Foswiki::Iterator::NumberRangeIterator->new(
        _numRevisions( $meta, $attachment ), 1 );
}

# Implement Foswiki::Store
sub getNextRevision {
    my ( $this, $meta ) = @_;

    return _numRevisions($meta) + 1;
}

# Implement Foswiki::Store
sub getRevisionDiff {
    my ( $this, $meta, $rev2, $contextLines ) = @_;

    my $rev1 = $meta->getLoadedRev();
    my @list;
    my ($text1) = _getRevision( $meta, undef, $rev1 );
    my ($text2) = _getRevision( $meta, undef, $rev2 );

    my $lNew = _split($text1);
    my $lOld = _split($text2);
    require Algorithm::Diff;
    my $diff = Algorithm::Diff::sdiff( $lNew, $lOld );

    foreach my $ele (@$diff) {
        push @list, $ele;
    }
    return \@list;
}

# Implement Foswiki::Store
sub getVersionInfo {
    my ( $this, $meta, $rev, $attachment ) = @_;

    my $info = $this->askListenersVersionInfo( $meta, $rev, $attachment );
    unless ($info) {

        $info = {};
        my $df;
        my $nr = _numRevisions( $meta, $attachment );
        if ( $rev && $rev > 0 && $rev < $nr ) {
            $df = _historyFile( $meta, $attachment, $rev );
            unless ( -e $df ) {
		# May arise if the history is not continuous, or if
		# there is no history
                $df = _latestFile( $meta, $attachment );
                $rev = $nr;
            }
        }
        else {
            $df = _latestFile( $meta, $attachment );
            $rev = $nr;
        }
        unless ($attachment) {

            # if it's a topic, try and retrieve TOPICINFO
            _getTOPICINFO( $df, $info );
        }
        $info->{date}    = _getTimestamp($df);
        $info->{version} = $rev;
        $info->{comment} = '' unless defined $info->{comment};
        $info->{author} ||= $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID;
    }

    return $info;
}

# Implement Foswiki::Store
sub saveAttachment {
    my ( $this, $meta, $name, $stream, $cUID, $comment ) = @_;

    _saveDamage( $meta, $name );

    my $rn = _numRevisions( $meta, $name ) + 1;
    my $verb = ( $meta->hasAttachment($name) ) ? 'update' : 'insert';

    my $latest = _latestFile( $meta, $name );
    _saveStream( $latest, $stream );
    my $hf = _historyFile( $meta, $name, $rn );
    _mkPathTo($hf);
    #File::Copy::copy( $latest, $hf )
    #  or die "Versatile: failed to copy $latest to $hf: $!";

    _recordChange( $meta, $cUID, $rn );

    $this->tellListeners(
        verb          => $verb,
        newmeta       => $meta,
        newattachment => $name
    );

    return $rn;
}

# Implement Foswiki::Store
sub saveTopic {
    my ( $this, $meta, $cUID, $options ) = @_;

    my $topicId = $this->_topicId($meta->_web, $meta->_topic);
    my $verb = $topicId > 0 ? 'update' : 'insert';
#    my $rn = _numRevisions( $meta ) + 1;

    # Fix TOPICINFO
    my $ti = $meta->get('TOPICINFO');
    # $ti->{version} = $rn;
    $ti->{date}    = $options->{forcedate} || time;
    $ti->{author}  = $cUID;

    # Create new latest
#    my $latest = _latestFile( $meta );
#    _saveFile( $latest, $meta->getEmbeddedStoreForm() );
    #if ( $options->{forcedate} ) {
    #    utime( $options->{forcedate}, $options->{forcedate}, $latest )    # touch
    #      or die "Versatile: could not touch $latest: $!";
    #}

    # Create history file by copying latest (modification date
    # doesn't matter, so long as it's >= $latest)
    #my $hf = _historyFile( $meta, undef, $rn );
    #_mkPathTo($hf);
    # File::Copy::copy( $latest, $hf )
    #  or die "Versatile: failed to copy $latest to $hf: $!";

    my $extra = $options->{minor} ? 'minor' : '';
    # _recordChange( $meta, $cUID, $rn, $extra );

#    $this->tellListeners( verb => $verb, newmeta => $meta );

    return; # $rn;
}

# Implement Foswiki::Store
sub repRev {
    my ( $this, $meta, $cUID, %options ) = @_;

    _saveDamage($meta);

    my $rn = _numRevisions($meta);
    ASSERT( $rn, $meta->getPath ) if DEBUG;
    my $latest = _latestFile($meta);
    my $hf = _historyFile( $meta, undef, $rn );
    my $t = ( stat $latest )[9]; # SMELL: use TOPICINFO?
    unlink($hf);

    my $ti = $meta->get('TOPICINFO');
    $ti->{version} = $rn;
    $ti->{date}    = $options{forcedate} || time;
    $ti->{author}  = $cUID;

    _saveFile( $latest, $meta->getEmbeddedStoreForm() );
    if ( $options{forcedate} ) {
        utime( $options{forcedate}, $options{forcedate}, $latest )    # touch
          or die "Versatile: could not touch $latest: $!";
    }

    # Date on the history file doesn't matter so long as it's
    # >= $latest
    # File::Copy::copy( $latest, $hf )
    #  or die "Versatile: failed to copy $latest to $hf: $!";

    my @log = ( 'minor', 'reprev' );
    unshift( @log, $options{operation} ) if $options{operation};
    _recordChange( $meta, $cUID, $rn, join( ', ', @log ) );

    $this->tellListeners( verb => 'update', newmeta => $meta );

    return $rn;
}

# Implement Foswiki::Store
sub delRev {
    my ( $this, $meta, $cUID ) = @_;

    _saveDamage($meta);

    my $rev = _numRevisions($meta);
    if ( $rev <= 1 ) {
        die 'Versatile: Cannot delete initial revision of '
          . $meta->web . '.'
          . $meta->topic;
    }

    my $hf = _historyFile( $meta, undef, $rev );
    unlink $hf;

    # Get the new top rev - which may or may not be -1, depending if
    # the history is complete or not
    my $cur = _numRevisions($meta);
    $hf = _historyFile( $meta, undef, $cur );
    my $thf = _latestFile($meta);

    # Copy it up to the latest file, then refresh the time on the history
    # File::Copy::copy( $hf, $thf )
    #  or die "Versatile: failed to copy to $thf: $!";
    utime( undef, undef, $hf )    # touch
      or die "Versatile: could not touch $hf: $!";

    # reload the topic object
    $meta->unload();
    $meta->loadVersion();

    $this->tellListeners( verb => 'update', newmeta => $meta );

    _recordChange( $meta, $cUID, $rev );

    return $rev;
}

# Implement Foswiki::Store
sub atomicLockInfo {
    my ( $this, $meta ) = @_;
    my $filename = _getData($meta) . '.lock';
    if ( -e $filename ) {
        my $t = _readFile($filename);
        return split( /\s+/, $t, 2 );
    }
    return ( undef, undef );
}

# It would be nice to use flock to do this, but the API is unreliable
# (doesn't work on all platforms)
sub atomicLock {
    my ( $this, $meta, $cUID ) = @_;
    my $filename = _getData($meta) . '.lock';
    _saveFile( $filename, $cUID . "\n" . time );
}

# Implement Foswiki::Store
sub atomicUnlock {
    my ( $this, $meta, $cUID ) = @_;

    my $filename = _getData($meta) . '.lock';
    unlink $filename
      or die "Versatile: failed to delete $filename: $!";
}


sub _fieldId {
    my ($this, $metatype, $metamember, $metakey) = @_;
    return 0 unless defined $metatype && $metatype ne ''; # Cannot be blank but next two can
    return 0 unless defined $metakey;

    my $hasMetamember = 1;
    if(!defined $metamember) {
        $hasMetamember = 0;
        $metamember = '';
    }
    # print STDERR "_fieldId#1 $metatype, $hasMetamember, $metamember, $metakey, $this->{fields}->{$metatype}->{$hasMetamember}->{$metamember}->{$metakey}\n";
     
    return $this->{fields}->{$metatype}->{$hasMetamember}->{$metamember}->{$metakey} if defined $this->{fields}->{$metatype}->{$hasMetamember}->{$metamember}->{$metakey};

    my $sth = $this->{fieldIdSt};
    $sth->execute($metatype,$hasMetamember,$metamember,$metakey);
    
    my @row = $sth->fetchrow_array();
#    print STDERR "_fieldId $metatype, $hasMetamember, $metamember, $metakey, $row[0]\n";
    return undef if !@row;
    # print STDERR "_fieldId#2 $metatype, $hasMetamember, $metamember, $metakey, $row[0]\n";
    return $this->{fields}->{$metatype}->{$hasMetamember}->{$metamember}->{$metakey} = $row[0];
}

sub _insertField {
    my ($this, $metatype, $metamember, $metakey, $type) = @_;
    $type = 1 if !defined($type) || $type eq '';
    my $id = $this->_fieldId($metatype,$metamember,$metakey);
    # print STDERR "Xref Field Id! $id\n" if $type == 2;
    return $id if defined $id;

    my $hasMetamember = 1;
    if(!defined $metamember) {
        $hasMetamember = 0;
        $metamember = '';
    }
    # SMELL: Need to add code to check if the name (metamember when hasMetamember) is a possible ForeLink. This in turn needs the possibility of being renamed
    # SMELL: E.g. a META:FIELD{name="StartDate" and StartDate was renamed to StartingDate then the relevant field-ids need to be updated
    # SMELL: Actually not, the Fields table is deliberately small. Therefore, it's easy to scan and rename the metamember parts of this table and not bother adding it to the xref table as a ForeLink
    # SMELL: This impacts the idea that the fields table is immutable - it clearly isn't. I therefore need to re-load the fields table at the beginning of each FW transaction
    $this->{dbh}->do("insert into fields (metatype, hasMetamember, metamember, metakey, type) values (?, ?, ?, ?, ?)",{},$metatype,$hasMetamember,$metamember,$metakey, $type);
    
    $this->{fields}->{$metatype}->{$hasMetamember}->{$metamember}->{$metakey} = $this->{dbh}->last_insert_id(undef,undef,undef,undef);    
    # $this->{fields}{$metatype}{$hasMetamember}{$metamember}{$metakey}{type} = $type;
    # print STDERR "Xref Field Id: $this->{fields}->{$metatype}->{$hasMetamember}->{$metamember}->{$metakey}\n" if $type == 2;
    return $this->{fields}->{$metatype}->{$hasMetamember}->{$metamember}->{$metakey};
}
sub _insertAccess {
    my ($this, $fobId, $accessId, $ad, $rwt, $mode) = @_;

    print STDERR "$fobId, $accessId, $ad, $rwt, $mode\n";
    my $modeId = $this->_insertField('_access_mode',undef,$mode);
    my $permit = $ad eq 'ALLOW' ? 1 : 0; # If not allowed then it's denied! 
    my $priority = $rwt eq 'TOPIC' ? 0x10 : 0;
    $priority += $rwt eq 'WEB' ? 0x20 : 0;
    $priority += $rwt eq 'ROOT' ? 0x30 : 0;
    $priority += $ad eq 'ALLOW' ? 1 : 0;

    print STDERR "$fobId, $accessId, $modeId, $priority, $permit\n";
    $this->{dbh}->do("insert into access (fobid, access_id, mode, priority, permit) values (?, ?, ?, ?, ?)", {}, $fobId, $accessId, $modeId, $priority, $permit);
 
    return;
}

sub _insertValue {
    my ($this, $fobId, $fieldId, $value, $baseWebId) = @_;
    
    my $ducktype = 4;
    my $epoch;

    $ducktype = 1 if $value && $value =~ /$this->{Number}/; # Number and string
    
    if($ducktype == 4 && $fieldId != $this->{fields}->{_text}->{1}->{''}->{''}) {
        $epoch = Foswiki::Time::parseTime(substr($value,0,100));
        $ducktype = 2 if $epoch;
    }
    
    my $v = $value;
    
    # NOTE: The metamember ('name') of the field-id is not scanned as a possible ForeLink, but fields is a small table so just scan that instead
    my $tagEnd = '[{%]';

    my %count = ( verbatim => 0, literal => 0, pre => 0, noautolink => 0);
    ELEMENT:
    while($v =~
        m/
            (^\s*<(\/)?(?i:(verbatim|literal|pre|noautolink))\b[^>]*>\s*$) |
            (^(?:\t|\ \ \ )+\*\s+(Set|Local)\s+($Foswiki::regex{tagNameRegex})\s*=\s*) |
            (\[\[[^\n]*?(?:\]\[|\]\])) |
            ((%MAINWEB%|%SYSTEMWEB%|%USERSWEB%|%WEB%|$Foswiki::regex{webNameRegex})(\.$Foswiki::regex{topicNameRegex})) |
            (%$Foswiki::regex{tagNameRegex}$tagEnd) |
            ($Foswiki::regex{topicNameRegex}) 
        /xmsgo) {
        my ($start, $end, $block, $blockend, $blockstart, $setvar, $setlocal, $settag, $squab, $wtlink, $web, $wtopic, $tag, $tlink) = ($-[0], $+[0], $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
        #print STDERR ($start . " (" . substr($v,$start, 20) . ")\n") if $fobId == 11 && $fieldId == 1;
        if($block) {
            # print STDERR "$fobId $fieldId <> $blockstart ($start, $end)\n";
            if($blockend) {
                $count{$blockstart}--;
            }
            else {
                $count{$blockstart}++;
            }
            next ELEMENT;
        }
        my $bcounts = ($count{verbatim} > 0 ? '+verbatim':'').($count{literal} > 0 ? '+literal':'').($count{pre} > 0 ? '+pre':'').($count{noautolink} > 0 ? '+noautolink':'');
        if($setvar) {
            # The Set/Local pref we've captured was only to ignore it. The $settag will be noted elsewhere as a preference so it's already Xreffed in a manner of speaking
            # The value part of the Set/local is further scanned for various Xref items
            # print STDERR "$fobId $fieldId == $setlocal $settag ($start, $end) $bcounts\n";
            next ELEMENT;
        }
        if($tlink) {
            $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                               $fobId,$fieldId,
                               $this->_insertTopic($baseWebId,$tlink,1),0);
            next ELEMENT;
        }
        if($wtlink) {
            $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                                           $fobId, $fieldId,
                                           $this->_insertTopic($baseWebId, $wtlink, 1), 0);
            # print STDERR "$fobId $fieldId +t $tlink ($start, $end) $bcounts\n";
            # print STDERR "$fobId $fieldId wt $wtlink ($start, $end) $bcounts\n";
            next ELEMENT;
        }
        if($tag) {
            $this->_insertXref($this->_insertField('_foreTag',$bcounts,''),
                                           $fobId, $fieldId,
                                           $this->_insertTopic($baseWebId, $tag, 1), 0);
            # print STDERR "$fobId $fieldId %% $tag ($start, $end) $bcounts\n";
            next ELEMENT;
        }
        if($squab) {
            # print STDERR "$fobId $fieldId [] $squab ($start, $end) $bcounts\n";
            my ($sqlink) = $squab =~ m/\[\[([^\n]*?)(?:\]\[|\]\])/;
            if($sqlink =~ m/^([ $Foswiki::regex{mixedAlpha}$Foswiki::regex{numeric}]+)$/ && $sqlink =~ m/ /) { # (?:\s+)
                my $ww = $sqlink;
                $ww =~ s/\s//g;
                my $w2 = $sqlink;
                $w2 =~ s/\s*([$Foswiki::regex{mixedAlpha}$Foswiki::regex{numeric}]+)\s*/ucfirst($1)/ge;
                $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                                   $fobId,$fieldId,
                                   $this->_insertTopic($baseWebId, $w2, 1), 0);
                # print STDERR "$fobId $fieldId !!!! [$sqlink][$ww][$w2]\n";
                next ELEMENT;
            }
            # print STDERR "$fobId $fieldId [+ $sqlink\n";
            SQUAB_ELEMENT:
            while($sqlink =~ 
                    m/
                    ((%MAINWEB%|%SYSTEMWEB%|%USERSWEB%|%WEB%|$Foswiki::regex{webNameRegex})(\.$Foswiki::regex{topicNameRegex})) |
                    (%$Foswiki::regex{tagNameRegex}$tagEnd) |
                    ($Foswiki::regex{topicNameRegex}) 
                /xmsg) {
                    my ($start, $end, $wtlink, $web, $wtopic, $tag, $tlink) = ($-[0], $+[0], $1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
                    if($tlink) {
                        # print STDERR "$fobId $fieldId []+t $tlink ($start, $end)\n";
                        $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                                           $fobId,$fieldId,
                                           $this->_insertTopic($baseWebId,$tlink,1),0);
                        next SQUAB_ELEMENT;
                    }
                    if($wtlink) {
                        $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                                           $fobId,$fieldId,
                                           $this->_insertTopic($baseWebId,$wtlink,1),0);
                        # print STDERR "$fobId $fieldId []wt $wtlink ($start, $end)\n";
                        next SQUAB_ELEMENT;
                    }
                    if($tag) {
                        $this->_insertXref($this->_insertField('_foreTag',$bcounts,''),
                                           $fobId,$fieldId,
                                           $this->_insertTopic($baseWebId,$tag,1),0);
                        # print STDERR "$fobId $fieldId []%% $tag ($start, $end)\n";
                        next SQUAB_ELEMENT;
                    }
            }
            next ELEMENT;
        }
    }

    push(@{$this->{values_string}}, [$fobId, $fieldId, $ducktype] );
    push(@{$this->{values_string_v}}, $value );

    if($ducktype == 1) {
        push(@{$this->{values_double}}, [$fobId, $fieldId] );
        push(@{$this->{values_double_v}}, $value );
    }
    
    if($ducktype == 2) {
        my $date = Foswiki::Time::formatTime($epoch, '$year-$mo-$day $hours:$minutes:$seconds');
        push(@{$this->{values_datetime}}, [$fobId, $fieldId] );
        push(@{$this->{values_datetime_v}}, $date );
    }

    $this->_insertFlush();    
}

sub _insertFlush {
    my ($this, $force) = @_;

    if($force || scalar @{$this->{values_string_v}} >= 1000) {
        my $sql = "insert into values_string values ";
        for my $cols (@{$this->{values_string}}) {
            my ($fobId, $fieldId, $ducktype) = @$cols;
            $sql .= "($fobId,$fieldId,$ducktype,?), ";
        }
        # print STDERR ("Entry string #" . scalar @{$this->{values_string_v}} . "\n");
        $sql = substr($sql,0,-2);
        $this->{dbh}->do($sql,{},(@{$this->{values_string_v}}));

        $this->{values_string} = [];
        $this->{values_string_v} = [];
    }
    
    if($force || scalar @{$this->{values_double_v}} >= 10000) {
        my $sql = "insert into values_double values ";
        for my $cols (@{$this->{values_double}}) {
            my ($fobId, $fieldId) = @$cols;
            $sql .= "($fobId,$fieldId,?), ";
        }
        #print STDERR ("Entry double #" . scalar @{$this->{values_double_v}} . "\n");
        $sql = substr($sql,0,-2);
        $this->{dbh}->do($sql,{},(@{$this->{values_double_v}}));

        $this->{values_double} = [];
        $this->{values_double_v} = [];
    }

    if($force || scalar @{$this->{values_datetime_v}} >= 10000) {
        my $sql = "insert into values_datetime values ";
        for my $cols (@{$this->{values_datetime}}) {
            my ($fobId, $fieldId) = @$cols;
            $sql .= "($fobId,$fieldId,?), ";
        }
        # print STDERR ("Entry datetime #" . scalar @{$this->{values_datetime_v}} . "\n");
        $sql = substr($sql,0,-2);
        $this->{dbh}->do($sql,{},(@{$this->{values_datetime_v}}));

        $this->{values_datetime} = [];
        $this->{values_datetime_v} = [];
    }    
}

sub _insertXref {
    my ($this, $reason, $fobId, $field_Id, $xref_Id, $xref_field) = @_;

    # print STDERR "Xref: $fobId, $fieldId, $xref_id, $xref_field, $reason\n";    
    $this->{dbh}->do("insert ignore into xref (fobid, field_id, xref_fobid, xref_field_id, reason) values (?, ?, ?, ?, ?)", {}, $fobId, $field_Id, $xref_Id, $xref_field, $reason);
}

sub _nextFobId {
    my ($this) = @_;
    $this->{dbh}->do("insert into fobID values (NULL)",{}); #smell exceptions!
    return $this->{dbh}->last_insert_id(undef,undef,undef,undef);
}

# if $topic is passed as blank then create a shadow topic
sub _insertTopic {
    my ($this, $webId, $topic, $forward) = @_;

    my $id = $this->_topicId($webId,$topic);
    my $pid = $id;
    $pid = '' if !defined $pid;
    # print STDERR "IT: $webId, $topic, $pid\n";    
    return $id if $id;

    my $fobId = $this->_nextFobId();
    my $type = $topic ne '' ? 4 : 5;
    $type += 16 if $forward; # SMELL: Strictly not valid if a shadow topic, just needs an ASSERT
    $topic = sprintf("FobT%016x",$fobId) if $topic eq '';

    $this->{dbh}->do("insert into FOB (fobid, type, webid, name) values (?, ?, ?, ?)",{},$fobId, $type, $webId, $topic);
    return $fobId;
}

# Given a web *ID* but a topic *NAME* return the cached fobId of the topic or go and look it up!
sub _topicId {
    my ( $this, $webId, $topic ) = @_;

    return $this->{topics}{$webId}{$topic} if $this->{topics}{$webId}{$topic};

    my $sth = $this->{topicIdSt};
    $sth->execute($webId,$topic);
    
    my @row = $sth->fetchrow_array();
    $this->{topics}{$webId}{$topic} = @row ? $row[0] : 0;
    return $this->{topics}{$webId}{$topic};
}

# Implement Foswiki::Store

sub webExists {
    my ( $this, $web ) = @_;
    return $this->_webId($web) > 0 ? 1 : 0;
}

sub _webId {
    my ( $this, $web ) = @_;

    return 0 unless defined $web;
    $web =~ s#\.#/#go;
    
    return $this->{webs}{$web} if $this->{webs}{$web};

    my $sth = $this->{webIdSt};
    $sth->execute($web);
    
    my @row = $sth->fetchrow_array();
    my $fobId = @row ? $row[0] : 0;
    # Note that not-found webs are cached as not found
    # Needs to change - could be created/renamed and we would never find it
    # Unless I can flush this cache for each request
    $this->{webs}{$web} = $fobId;
    return $fobId;
}

sub _insertWeb {
    my ($this, $web, $forward) = @_;
    return 0 unless defined $web;
    $web =~ s#\.#/#go;
    my $id = $this->_webId($web);
    return $id if $id;

    my $fobId = $this->_nextFobId();
    my $type = $web ne '' ? 1 : 2;
    $type += 16 if $forward; 
    $web = sprintf("Fobw%x",$fobId) if $web eq '';
    
    $this->{dbh}->do("insert into FOB (fobid, type, webid, name) values (?, ?, 0, ?)",{},$fobId, $type, $web);
    return $fobId;
}

sub _getDBH {
    my ($this) = @_;
    return $this->{dbh};
}

# Implement Foswiki::Store
sub topicExists {
    my ( $this, $web, $topic ) = @_;

    return 0 unless defined $web && $web ne '';
    $web =~ s#\.#/#go;
    return 0 unless defined $topic && $topic ne '';

    my $dbh = $this->{dbh};
    my $webId = $this->_webId($web);
    my $sth = $dbh->prepare("select fobid from FOB where webid = ? AND name = ?");
    $sth->execute($webId,$topic);

    my @row = $sth->fetchrow_array();
    return @row ? $row[0] : 0;
#    $this->{webs}{$web} = @row ? $row[0] : 0;
}

# Implement Foswiki::Store
sub getApproxRevTime {
    my ( $this, $web, $topic ) = @_;

    return ( stat( _latestFile( $web, $topic ) ) )[9] || 0;
}

# Implement Foswiki::Store
sub eachChange {
    my ( $this, $meta, $since ) = @_;

    my $file = _getData($meta->web) . '/.changes';
    require Foswiki::ListIterator;

    if ( -r $file ) {

        # Could use a LineIterator to avoid reading the whole
        # file, but it hardly seems worth it.
        my @changes =
          map {

            # Create a hash for this line
            {
                topic => Foswiki::Sandbox::untaint(
                    $_->[0], \&Foswiki::Sandbox::validateTopicName
                ),
                user     => $_->[1],
                time     => $_->[2],
                revision => $_->[3],
                more     => $_->[4]
            };
          }
          grep {

            # Filter on time
            $_->[2] && $_->[2] >= $since
          }
          map {

            # Split line into an array
            my @row = split( /\t/, $_, 5 );
            \@row;
          }
          reverse split( /[\r\n]+/, _readFile($file) );

        return Foswiki::ListIterator->new( \@changes );
    }
    else {
        my $changes = [];
        return Foswiki::ListIterator->new($changes);
    }
}

# Implement Foswiki::Store
sub eachAttachment {
    my ( $this, $meta ) = @_;

    my $dh;
    opendir( $dh, _getPub($meta) ) or return ();
    my @list = grep { !/^[.*_]/ && !/,pfv$/ } readdir($dh);
    closedir($dh);

    require Foswiki::ListIterator;
    return new Foswiki::ListIterator( \@list );
}

# Implement Foswiki::Store
sub eachTopic {
    my ( $this, $meta ) = @_;

    my $web = $meta->web;
    my $dbh = $this->{dbh};
    my $webId = $this->_webId($web);
    my $sth = $dbh->prepare("select fobid from FOB where webid = ?");
    $sth->execute($webId);

    my @row = $sth->fetchrow_array();

    # the name filter is used to ensure we don't return filenames
    # that contain illegal characters as topic names.
    #my @list =
    #  map { /^(.*)\.txt$/; $1; }
    #  sort
    #  grep { !/$Foswiki::cfg{NameFilter}/ && /\.txt$/ } readdir($dh);
    #closedir($dh);

    require Foswiki::ListIterator;
    return; # new Foswiki::ListIterator( \@list );
}

# Implement Foswiki::Store
sub eachWeb {
    my ( $this, $meta, $all ) = @_;

    # Undocumented; this fn actually accepts a web name as well. This is
    # to make the recursion more efficient.
    my $web = ref($meta) ? $meta->web : $meta;

    my $dir = $Foswiki::cfg{DataDir};
    $dir .= '/' . $web if defined $web;
    my @list;
    my $dh;

    if ( opendir( $dh, $dir ) ) {
        @list = map {
            Foswiki::Sandbox::untaint( $_, \&Foswiki::Sandbox::validateWebName )
          }

          # The -e on the web preferences is used in preference to a
          # -d to avoid having to validate the web name each time. Since
          # the definition of a Web in this handler is "a directory with a
          # WebPreferences.txt in it", this works.
          grep { !/\./ && -e "$dir/$_/$Foswiki::cfg{WebPrefsTopicName}.txt" }
          readdir($dh);
        closedir($dh);
    }

    if ($all) {
        my $root = $web ? "$web/" : '';
        my @expandedList;
        while ( my $wp = shift(@list) ) {
            push( @expandedList, $wp );
            my $it = $this->eachWeb( $root . $wp, $all );
            push( @expandedList, map { "$wp/$_" } $it->all() );
        }
        @list = @expandedList;
    }
    @list = sort(@list);
    require Foswiki::ListIterator;
    return new Foswiki::ListIterator( \@list );
}

# Implement Foswiki::Store
sub remove {
    my ( $this, $cUID, $meta, $attachment ) = @_;
    my $f;
    if ( $meta->topic ) {

        # Topic or attachment
        unlink( _latestFile( $meta, $attachment ) );
        _rmtree( _historyDir( $meta, $attachment ) );
	_rmtree( _getPub($meta) ) unless ($attachment); # topic only
    }
    else {

        # Web
        _rmtree( _getData($meta) );
	_rmtree( _getPub($meta) );
    }

    $this->tellListeners(
        verb          => 'remove',
        oldmeta       => $meta,
        oldattachment => $attachment
    );

    # Only log when deleting topics or attachment, otherwise we would re-create
    # an empty directory with just a .changes.
    if ($attachment) {
        _recordChange( $meta, $cUID, 0, 'Deleted attachment ' . $attachment );
    }
    elsif ( my $topic = $meta->topic ) {
        _recordChange( $meta, $cUID, 0, 'Deleted ' . $topic );
    }
}

# Implement Foswiki::Store
sub query {
    my ( $this, $query, $inputTopicSet, $session, $options ) = @_;

    my $engine;
    if ( $query->isa('Foswiki::Query::Node') ) {
        unless ( $this->{queryObj} ) {
            my $module = $Foswiki::cfg{Store}{QueryAlgorithm};
            eval "require $module";
            die
"Bad {Store}{QueryAlgorithm}; suggest you run configure and select a different algorithm\n$@"
              if $@;
            $this->{queryObj} = $module->new();
        }
        $engine = $this->{queryObj};
    }
    else {
        ASSERT( $query->isa('Foswiki::Search::Node') ) if DEBUG;
        unless ( $this->{searchQueryObj} ) {
            my $module = $Foswiki::cfg{Store}{SearchAlgorithm};
            eval "require $module";
            die
"Bad {Store}{SearchAlgorithm}; suggest you run configure and select a different algorithm\n$@"
              if $@;
            $this->{searchQueryObj} = $module->new();
        }
        $engine = $this->{searchQueryObj};
    }

    no strict 'refs';
    return $engine->query( $query, $inputTopicSet, $session, $options );
    use strict 'refs';
}

# Implement Foswiki::Store
sub getRevisionAtTime {
    my ( $this, $meta, $time ) = @_;

    my $hd = _historyDir($meta);
    my $d;
    unless (opendir( $d, $hd )) {
	return 1 if ( $time >= ( stat(_latestFile($meta)) )[9] );
	return 0;
    }
    my @revs = reverse sort grep { /^[0-9]+$/ } readdir($d);
    closedir($d);

    foreach my $rev (@revs) {
        return $rev if ( $time >= ( stat("$hd/$rev") )[9] );
    }
    return undef;
}

# Implement Foswiki::Store
sub getLease {
    my ( $this, $meta ) = @_;

    my $filename = _getData($meta) . '.lease';
    my $lease;
    if ( -e $filename ) {
        my $t = _readFile($filename);
        $lease = { split( /\r?\n/, $t ) };
    }
    return $lease;
}

# Implement Foswiki::Store
sub setLease {
    my ( $this, $meta, $lease ) = @_;

    my $filename = _getData($meta) . '.lease';
    if ($lease) {
        _saveFile( $filename, join( "\n", %$lease ) );
    }
    elsif ( -e $filename ) {
        unlink $filename
          or die "Versatile: failed to delete $filename: $!";
    }
}

# Implement Foswiki::Store
sub removeSpuriousLeases {
    my ( $this, $web ) = @_;
    my $webdir = _getData($web) . '/';
    if ( opendir( my $W, $webdir ) ) {
        foreach my $f ( readdir($W) ) {
            my $file = $webdir . $f;
            if ( $file =~ /^(.*)\.lease$/ ) {
                if ( !-e "$1,pfv" ) {
                    unlink($file);
                }
            }
        }
        closedir($W);
    }
}

#############################################################################
# PRIVATE FUNCTIONS
#############################################################################
1;
__END__
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
