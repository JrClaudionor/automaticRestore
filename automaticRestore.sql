USE [any_other_database] 

go 

/*

	PROCEDURE PARA FECHAR AS CONEXÕES QUE IMPEDIRIAM O RESTORE DO BANCO DE DADOS

*/

CREATE PROC [dbo].[spKillProcess] @databaseName NVARCHAR(max) 
AS 
  BEGIN 
      SET nocount ON 

      DECLARE users CURSOR FOR 
        SELECT spid 
        FROM   master..sysprocesses 
        WHERE  Db_name(dbid) = @databaseName 
      DECLARE @SPID INT, 
              @STR  VARCHAR(255) 

      OPEN users 

      FETCH next FROM users INTO @SPID 

      WHILE @@FETCH_STATUS <> -1 
        BEGIN 
            IF @@FETCH_STATUS = 0 
              BEGIN 
                  SET @STR = 'KILL ' + CONVERT(VARCHAR, @SPID) 

                  EXEC (@STR) 
              END 

            FETCH next FROM users INTO @SPID 
        END 

      DEALLOCATE users 
  END 

/*

	PROCEDURE QUE AUTOMATIZA O RESTORE

*/

CREATE PROC spRestore(@databaseName NVARCHAR(max)) 
AS 
  BEGIN 
	DECLARE @directory TABLE 
	( 
		_filename VARCHAR(300), 
		a           INT, 
		b           INT, 
		dtTime    DATETIME, 
		bkpType     CHAR(1) 
	) 
	DECLARE @script               NVARCHAR(max) = 'exec [any_other_database].dbo.spKillProcess ''' 
												+ @databaseName + '''; ', 
		@scriptConfig         NVARCHAR(max) = N'	WITH  FILE = 1,   MOVE ''<logical_file_name_in_backup>_Data''  TO ''<MDF FILE PATH>' 
											+ @databaseName 
											+ '.MDF''  ,MOVE ''<logical_file_name_in_backup>_Log''  TO ''<LOG FILE PATH>' 
											+ @databaseName 
											+ '.LDF''   ,NOUNLOAD ,STATS = 5 ', 
		@absolutePath VARCHAR(8000) = '<BACKUP REPOSITORY PATH>'
		@counter        INT = 0 


/*

	ATENÇÃO ÀS VULNERABILIDADES DA XP_DIRTREE:
	https://kc.mcafee.com/corporate/index?page=content&id=KB91036&locale=en_US

	AS COLUNAS "a" E "b" FORAM CRIADAS PARA RECEBER OS RETORNOS NÃO ESSENCIAIS (PARA O NOSSO CASO) DA XP_DIRTREE.

	USO A XP_DIRTREE PARA LISTAR OS ARQUIVOS QUE EU QUERO USAR NOS BACKUPS.
	NO MEU CASO, POSSUO UMA PASTA QUE RECEBE APENAS OS ARQUIVOS QUE EU VOU UTILIZAR NO RESTORE.
	PRETENDO MELHORAR ISSO NO FUTURO.

*/
	
    INSERT @directory 
           (_filename, 
            a, 
            b) 
    EXEC xp_dirtree   	@absolutePath, 
			2, 
			1 


/*

	NO MEU CASO, OS BACKUPS QUE EU TRABALHO POSSUEM UMA NOMENCLATURA PADRÃO, COMO ESSE:
	NOMEBANCO_TIPOBACKUP_YYYY_MM_DD_HHmmss.BAK
	OBS:. OS TIPOS DE BACKUP SÃO FULL, DIFERENCIAL E LOG.

	POR ISSO CRIEI UMA LÓGICA PARA COLHER ESTAS INFORMAÇÕES E USÁ-LAS PARA ORDENAR O RESTORE CORRETAMENTE.
	NO MEU CASO JÁ É O SUFICIENTE, MAS PRETENDO MELHORAR ISSO DE ALGUMA FORMA NO FUTURO TAMBÉM.

*/

    UPDATE d 
    SET    dtTime = directory._date + ' ' 
                      + LEFT(directory._hour, 2) + ':' 
                      + LEFT(RIGHT(directory._hour, 7), 2) + ':' 
                      + LEFT(RIGHT(directory._hour, 5), 2) + '.' 
                      + RIGHT(directory._hour, 3), 
           bkpType = directory.bkpType 
    FROM   (SELECT Replace(
					LEFT(
						  RIGHT(
								 Replace(_filename, '.BAK', '')
						  , 20)
					, 10) 
		   , '_', '-') AS _date, 
                   Replace(
					RIGHT(
						   Replace(_filename, '.BAK', '')
					, 9)
		   , '_', '-') AS _hour, 
                   _filename, 
                   CASE 
                     WHEN Charindex('FULL', _filename) > 0 THEN 'F' 
                     WHEN Charindex('DIFF', _filename) > 0 THEN 'D' 
                     WHEN Charindex('LOG', _filename) > 0 THEN 'L' 
                     ELSE NULL 
                   END 
                   as bkpType 
            FROM   @directory) AS directory 
           INNER JOIN @directory d 
                   ON d._filename = directory._filename 

    BEGIN 
        DECLARE @_filename VARCHAR(300), 
                @bkpType     CHAR(1) 

        DECLARE bkpList CURSOR FOR 
         
		 SELECT _filename, 
                 bkpType 
          FROM   @directory 
          ORDER  BY dtTime 

        OPEN bkpList 

        FETCH next FROM bkpList INTO 	@_filename, 
				   	@bkpType 

        WHILE @@fetch_status = 0 
          BEGIN 
              SET @counter= @counter+ 1 
              SET @script = @script + N' RESTORE DATABASE ' + @databaseName 
                            + ' FROM DISK = ''' + @absolutePath 
                            + '\' + @_filename + '''' + @scriptConfig 

              IF( @counter< @@CURSOR_ROWS ) 
                BEGIN 
                    SET @script = @script + N' ,NORECOVERY ' 
                END 

              IF( @bkpType = 'F' ) 
                BEGIN 
                    SET @script = @script + N' ,REPLACE	' 
                END 

              FETCH next FROM bkpList INTO @_filename, @bkpType 
          END 

        CLOSE bkpList 

        DEALLOCATE bkpList 
    END 

	/*

		VERIFIQUE O SCRIPT GERADO, SE NECESSÁRIO:
		PRINT @script

	*/
    EXEC sp_executesql @script 
END 
