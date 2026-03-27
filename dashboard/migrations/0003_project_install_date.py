from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0002_widen_state_and_status_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="project",
            name="install_date",
            field=models.DateField(blank=True, null=True),
        ),
    ]
