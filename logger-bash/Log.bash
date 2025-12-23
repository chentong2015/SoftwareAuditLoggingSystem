#! /bin/bash
#
# PURPOSE:
#   This bash module contains commands to log several action from an unique place
#
# EXPECTED ENV VARIABLES:
#  Mandatory:
#     BASH_HELPER: path of Bash-Helper framework
#     LOG_FILE: current log file to use
#
#  Optional:
#      DB_LOG_FILE: current log file to log DB action
#      APP_LOG_FILE: current log file to log running application action
#
# MAIN COMMANDS:
#
#   Log Event with date and hour in main log pointed by LOG_FILE
#>>>>>>>> logEvent EventDescription
#
#   Log Event as Info with date and hour in main log pointed by LOG_FILE
#>>>>>>>> logInfo EventDescription
#
#   Log Event as Action with date and hour in main log pointed by LOG_FILE
#>>>>>>>> logAction EventDescription
#
#   Log Event as Error with date and hour in main log pointed by LOG_FILE
#>>>>>>>> logError EventDescription
#
#   Log Event as Info with date and hour in DB log pointed by DB_LOG_FILE
#>>>>>>>> logInfoInDBLog EventDescription
#
#   Log Event as Info with date and hour in main log pointed by LOG_FILE and DB log pointed by DB_LOG_FILE
#>>>>>>>> logInfoInMainAndDBLog EventDescription
#
#   Log Event as Info with date and hour in log pointed by APP_LOG_FILE
#>>>>>>>> logInfoInAppLog EventDescription

if [ -z "$BASH_HELPER" ]; then
  BASH_HELPER=./..
fi
. ${BASH_HELPER}/src/FileOperation.bash

GetLogFile(){
  local __result=$1
  eval $__result="$LOG_FILE"
}

logEvent() {
  local curLogFile
  GetLogFile curLogFile

  local eventDate=`date "+%Y-%m-%d  %H:%M:%S"`  
  if [ ! -z ${curLogFile} ]; then
    echo $eventDate" $*">>"$curLogFile"
  else
    echo $eventDate" $*"
  fi
}

logInfo() {
  logEvent "-I- $*"
}

logAction() {
  logEvent "-A- $*"
}

logError() {
  logEvent "-E- $*"
}

GetDBLogFile(){
  local __result=$1
  eval $__result="$DB_LOG_FILE"
}

GetAppLogFile(){
  local __result=$1
  eval $__result="$APP_LOG_FILE"
}


logInfoInMainAndDBLog() {
  GetLogFile mainLogFile
  GetDBLogFile DBLogFile

  local eventDate=`date "+%Y-%m-%d  %H:%M:%S"`
  
  if [ ! -z ${mainLogFile} ]; then
    echo $eventDate" -I- $*">>"$mainLogFile"
  else
    echo $eventDate' -I- '" $*"
  fi
  
  if [ ! -z ${DBLogFile} ]; then
    echo $eventDate" -I- $*">>"$DBLogFile"
  else
    echo $eventDate" -I- $*"
  fi    
}

logInfoInDBLog() {
  GetDBLogFile curLogFile

  local eventDate=`date "+%Y-%m-%d  %H:%M:%S"`
  
  if [ ! -z ${curLogFile} ]; then
    echo $eventDate" -I- $*">>"$curLogFile"
  else
    echo $eventDate" -I- $*"
  fi    
}

logInfoInAppLog() {
  GetAppLogFile curLogFile

  local eventDate=`date "+%Y-%m-%d  %H:%M:%S"`
  
  if [ ! -z ${curLogFile} ]; then
    echo $eventDate" -I- $*">>"$curLogFile"
  else
    echo $eventDate" -I- $*"
  fi    
}


logDebugAction() {
  local currentlogFile=$LOG_DEBUG

  local eventDate=`date "+%Y-%m-%d %H:%M:%S"`  
  if [ ! -z ${currentlogFile} ]; then
    echo 'Action;'$eventDate';'"$*"';'>>"$currentlogFile"
  else
    echo $eventDate" $*"
  fi
}

#export level='  '
logDebugActionStart() {
  local currentlogFile=$LOG_DEBUG

  local eventDate=`date "+%Y-%m-%d %H:%M:%S"`  
  if [ ! -z ${currentlogFile} ]; then
    echo 'Action ;'$eventDate';'"$level"'> '"$*"';'>>"$currentlogFile"
  fi
  level=$level'  '
}

logDebugActionEnd() {
  local currentlogFile=$LOG_DEBUG
  level="${level::-2}"
  local eventDate=`date "+%Y-%m-%d %H:%M:%S"`  
  if [ ! -z ${currentlogFile} ]; then
    echo 'Action ;'$eventDate';'"$level"'< '"$*"';'>>"$currentlogFile"
  fi

}

logDebugMeasure() {
  local currentlogFile=$LOG_DEBUG

  local eventDate=`date "+%Y-%m-%d %H:%M:%S"`  
  if [ ! -z ${currentlogFile} ]; then
    echo 'Measure;'$eventDate';'"$level"'----'"$*"';'>>"$currentlogFile"
  fi
}
