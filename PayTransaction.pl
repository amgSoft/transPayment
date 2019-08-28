#!/usr/bin/perl -w

use strict;
use DBI;
use Encode;
use MIME::Lite;

# Mysql connection data
my $USER_My = "dbname";
my $PASSWD_My = "dbpass";
my $INSTANCE_My = join ';','DBI:mysql:database=UTM5:127.0.0.1','mysql_read_default_group=perl','mysql_read_default_file=/etc/my.cnf';

# Oracle connection data
my $USER_Or = "dbname";
my $PASSWD_Or = "dbpass";
my $INSTANCE_Or = "DBI:Oracle:ACC";

sub crop($) {my $go=$_[0];$go=~s/^\s+//;$go=~s/\n+$//;$go=~s/\s+$//;return $go;};

################################################################################
#подпрограмма для отправки почты
sub mail($){
      my $text = $_[0];
      my $msg = MIME::Lite->new (
      From =>'UMT <UTM@domain.com.>',
      To =>'admin@domain.com.',
      Subject =>'ATTENTION - Payment.',
      Type => 'text/plain; charset=UTF-8',
      Data =>"$text"
      );
      $msg->send;
};

#Connection to DB Oracle
my $dbh1 = DBI->connect($INSTANCE_Or, $USER_Or, $PASSWD_Or, {PrintError => 1, RaiseError => 1,
                                                         LongReadLen => 64000, LongTruncOk => 0})
or die "Connection failed\n";

my $sth1 = $dbh1->prepare("SELECT   ffld14286,ffld14308,ffld14313,
                                    (ffld14315 - to_date('01.01.1970','DD.MM.YYYY')) * (86400),
                                    ffld14316
                              FROM info_new.inforg14275
                              WHERE TO_DATE(ffld14317,'DD.MM.YYYY')=TO_DATE(SYSDATE,'DD.MM.YYYY')
                              AND ffld14310='2'
                              AND (ffld14287 NOT LIKE '%VISA%')
                              AND (ffld14287 NOT LIKE '%MASTERCARD%')
                              AND (ffld14287 NOT LIKE '%MAESTRO%')
			      AND (ffld14287 NOT LIKE '%EUROCARD%')
                              AND (ffld14287 NOT LIKE '%Portmone.com%')
                              AND (ffld14287 NOT LIKE '%№ДНТ%')
                              AND (ffld14287 NOT LIKE '%CIRRUS%')
                              AND (ffld14287 NOT LIKE '%PRIVAT%')
                              AND (ffld14287 NOT LIKE '%fbank%')
                              AND (ffld14287 NOT LIKE '%248/15-%')
			      AND (ffld14287 NOT LIKE '%846/16%')
			      AND (ffld14287 NOT LIKE '%TYME%')");
$sth1->execute();

my $array_ref = $sth1->fetchall_arrayref();

# Connection to DB Mysql
my $dbh2 = DBI->connect($INSTANCE_My, $USER_My, $PASSWD_My, {PrintError => 1, RaiseError => 1, LongReadLen => 64000, LongTruncOk => 0}) or die "Connection failed\n";
my $sth2 = $dbh2->prepare(qq{SELECT UTM5.AcceptPay(?,?)});
my $sth3 = $dbh2->prepare(qq{SELECT UTM5.TotalPays()});
my $sth4 = $dbh2->prepare(qq{SELECT payers_log.contracts FROM payers_log});
my $sth5 = $dbh2->prepare(qq{INSERT INTO payers_log SET contracts=?});


################################ start ################################################
##Select contracts

$sth4->execute();
my %total_hashpays;
my $arr_contract = $sth4->fetchall_arrayref() or die "$sth4->errstr\n";
my ($i,$j);
for $i (0..$#{$arr_contract})
      {
       for $j (0..$#{$arr_contract->[$i]}){$total_hashpays{crop($arr_contract->[$i][$j])}=1;}
      }
my $rc = $sth4->finish;
####################################### end ##############################################

my $count_pays="0";
my ($total_sum,$count,$sumForDay,$dat,$tr_0,$tr_1,$tr_2,$tr_3,$tr_4,
    $sum,$meth,$dogovor,$DateOfPay,$currency);

my (%more_pays,%trab_0,%trab_1,%trab_2,%trab_3,%trab_4);

####Go to array
######################################start##################################################
foreach my $row (@$array_ref)
{
      ($sum,$meth,$dogovor,$DateOfPay,$currency)=@$row;
################# если сегодня платеж уже производился, то мы его пропускаем ######################
      if(exists($total_hashpays{crop($dogovor)})) {next;}
######## редактируем метот платежа ##############################################
      if ((crop($meth))==0){$meth="100"}else{$meth="101"} 
######################## ищим два платежа по одному контракту в одинь день
      if (exists($more_pays{crop($dogovor),crop($DateOfPay)}))
      {
            $trab_1{crop($dogovor)}=1;
            next;
      }

$sth2->execute($dogovor,$DateOfPay)or die $!,"\n";
my $acces = $sth2->fetchrow_arrayref;
###########
       if(@$acces[0]==0)
      {
            $trab_0{crop($dogovor)}=1;
            next;
      }
############### платеж уже произведен, повторного зачисления не производить #############
       if(@$acces[0]==1) {next;}  
######## проверяем правильность номера договора #################################
      
      if(@$acces[0]==2)
      {
            $trab_2{crop($dogovor)}=1;
            next;
      }
      if(@$acces[0]==3)
      {
            $trab_3{crop($dogovor)}=1;
            next;
      }  
########## если система обнариживает в билинге 2 одинаковых договора то отправляется сообщение менеджеру ####
      if (@$acces[0]==4)
      {
            $trab_4{crop($dogovor)}=1;
            next;
      }
###################################################################################      
################## обрабатываем платежи ##########################################
      if (@$acces[0]>4){
            #print "платим по договору $dogovor \n"; 
            system("/utm/bin/utm5_payment_tool -a '@$acces[0]' -b '$sum' -c '$currency' \\
                   -m '$meth' -t '$DateOfPay' -i 1 -L 'Plata za internet soglasno dogovory $dogovor'");
            $count_pays++;
            $total_sum += $sum;
            $sth5->execute($dogovor);
      }
$more_pays{crop($dogovor),crop($DateOfPay)}=1;
}
$rc = $sth1->finish;
$rc = $sth2->finish;
####################################################end################################
##########################получаем общую сумму за день ############################
if ($count_pays > 0){
      $sth3->execute();
      my $TotalForDay = $sth3->fetchrow_arrayref;
      @$TotalForDay[0] =~ m/^(\d+)\,(.+)\,(.+)$/;
      $count = $1;
      $sumForDay = $2;
      $dat = $3;
      $rc = $sth3->finish;
}
####Обрабатываем хеши ###########################################################################
foreach my $key (keys %trab_0)
{
      unless (exists($total_hashpays{crop($key)}))
      {
            $sth5->execute($key);
            $tr_0 .="$key\n";
      }
}
foreach my $key (keys %trab_1)
{
      unless (exists($total_hashpays{crop($key)}))
      {
            $sth5->execute($key);
            $tr_1 .="$key\n";    
      }
}
foreach my $key (keys %trab_2)
{
      unless (exists($total_hashpays{crop($key)}))
      {
            $sth5->execute($key);
            $tr_2 .="$key\n";      
      }
}
foreach my $key (keys %trab_3)
{
      unless (exists($total_hashpays{crop($key)}))
      {
            $sth5->execute($key);
            $tr_3 .="$key\n";            
      }
}
foreach my $key (keys %trab_4)
{
      unless (exists($total_hashpays{crop($key)}))
      {
            $sth5->execute($key);
            $tr_4 .="$key\n";                    
      }
}
$rc = $sth5->finish;
################################################################################
##################### Send email #######################################
&mail
      (
my $text = "
      -- ДАТА ТРАНЗАКЦИИ $dat --
      -- ЗАЧИСЛЕНО ПЛАТЕЖЕЙ $count_pays --
      -- НА ОБЩУЮ СУМУ $total_sum ГРН. --
      -- КОЛИЧЕСТВО ПЛАТЕЖЕЙ ЗА ДЕНЬ $count --
      -- СУМА ПЛАТЕЖЕЙ ЗА ДЕНЬ $sumForDay ГРН. --"
      ) if $count_pays > 0;

&mail
      (
my $err1 = "
-- Обнаружено два платежа по одному договору в одинь день, проверте правильность второго платежа  --
-- и зачислите его самостоятельно: --
$tr_1"
      ) if (defined($tr_1));
      
&mail
      (
my $err2 = "
-- УКАЗАНЫХ ДОГОВОРОВ В СИСТЕМЕ НЕ СУЩЕСТВУЕТ:--
$tr_2"
      ) if (defined($tr_2));


&mail
      (
my $err3 = "
-- ПО НИЖЕ УКАЗАНЫМ ДОГОВОРАМ, ЗАЧИСЛЕНИЯ ПРОИЗВОДИЛА НЕ СИСТЕМА ПЛАТЕЖЕЙ: --
$tr_3"
      ) if (defined($tr_3));

&mail
      (
my $err4 = "
-- Задублированый(е) номер(а) договоров в системе UTM: --
$dogovor"
      ) if (defined($tr_4));

&mail
      (
my $err0 = "
-- Платежи по ниже указанным договорам производились в старой системе Rad: --
$dogovor"
      ) if (defined($tr_0));
############################# END ##############################################
exit;
