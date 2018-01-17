Select d1.MemberID as "Member ID", FirstName as "First Name", LastName as "Last Name",  d1.DiagnosisID as "Most Severe Diagnosis ID", d1.DiagnosisDescription as "Most Severe Diagnosis Description", d2.DiagnosisCategoryID as "Category ID", d2.CategoryDescription as "Category Description", d2.CategoryScore as "Category Score", ISNull(d3.MostSevereCategory,1) as "Is Most Severe Category"
 	From (Select m.[MemberID], m.[FirstName], m.[LastName], md.[DiagnosisID], d.[DiagnosisDescription] FROM [Member] m 
	LEFT JOIN [MemberDiagnosis] md  ON m.[MemberID] = md.[MemberID] AND md.[DiagnosisID] = (SELECT MIN([DiagnosisId]) FROM [MemberDiagnosis] WHERE [MemberID] = md.[MemberID])
	LEFT JOIN [Diagnosis] d ON d.[DiagnosisID] = md.[DiagnosisID]) d1

LEFT JOIN 

(Select d.[DiagnosisID], dc.[DiagnosisCategoryID], dc.[CategoryDescription], dc.[CategoryScore] 
	FROM [DiagnosisCategoryMap] dcm 
	LEFT Join [Diagnosis] d ON d.[DiagnosisID] = dcm.[DiagnosisID]
	JOIN [DiagnosisCategory] dc  ON dc.[DiagnosisCategoryID] = dcm.[DiagnosisCategoryID]) d2
	
ON d1.[DiagnosisID] = d2.[DiagnosisID]

LEFT JOIN

(SELECT 
      md.MemberId, MIN(dc.DiagnosisCategoryID) as MostSevereCategory FROM
	  MemberDiagnosis md 
      LEFT JOIN DiagnosisCategoryMap dcm ON dcm.DiagnosisID = md.DiagnosisID
      LEFT JOIN DiagnosisCategory dc ON dcm.DiagnosisCategoryID = dc.DiagnosisCategoryID
    GROUP BY md.MemberId) d3
    
 ON d1.MemberID = d3.MemberID
