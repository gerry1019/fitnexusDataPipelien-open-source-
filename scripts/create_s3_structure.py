from utils.s3_utils import get_s3_client

folders = [
    "bronze/User/",
    "bronze/UserInfo/",
    "bronze/Booking/",
    "bronze/Payment/",
    "bronze/UserMembership/",
    "bronze/Session/",
    "bronze/TrainerSession/",
    "bronze/SessionFeedback/",
    "bronze/FitnessCenter/",
    "bronze/Staff/",
    "bronze/Address/",
    "bronze/WorkOutDetails/",
    "bronze/MembershipPlan/",
    "bronze/FitnessCenterSubscription/",
    "bronze/FitnessCenterMembershipPlan/",
]

def create_structure():
    s3 = get_s3_client()
    bucket = "fitness-lake"

    for folder in folders:
        s3.put_object(Bucket=bucket, Key=folder)
        print(f"🏗 Created folder: {folder}")

create_structure()
