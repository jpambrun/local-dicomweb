-- select "00080018" from read_vortex('*/*.vortex');


-- select "00080018" from '*/*.parquet';

-- select "00080028"->'$.Value[0]' from '*/*.parquet';




-- select "0020000D","00080018", "00100010", "" from '*/*.parquet';


CREATE OR REPLACE VIEW instances AS
SELECT
  TRY("00020010"->>'$.Value[0]')::VARCHAR as TransferSyntaxUID,
  TRY("00080018"->>'$.Value[0]')::VARCHAR as SOPInstanceUID,
  TRY("00080032"->>'$.Value[0]')::VARCHAR as AcquisitionTime,
  TRY("00200032"->>'$.Value')::VARCHAR as ImagePositionPatient,
  TRY(strptime("00080020"->>'$.Value[0]', '%Y%m%d'))::Date AS StudyDate,
  TRY("00080060"->>'$.Value[0]')::VARCHAR as Modality,
  privateTags->>'$.00090001.Value[0]' as FileName
FROM read_parquet('*/*.parquet', union_by_name = true);

SELECT * from instances ORDER BY StudyDate;

SELECT Modality, count(*) as cnt
FROM instances
GROUP BY Modality
ORDER by cnt DESC;


SELECT TRY(''->>'$.Value[0]')::VARCHAR;
